{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | The other half of an informer (paired with
-- "Kubernetes.Operator.Cache"): keeps the local Cache in sync with the API
-- server and feeds the Workqueue so the Controller's worker pool has
-- something to reconcile.
--
-- Deliberately /not/ an effect: there is nothing here a test would want to
-- swap out independently of 'Kubernetes.Operator.Client.KubeClient',
-- 'Kubernetes.Operator.Cache.Cache', and
-- 'Kubernetes.Operator.Workqueue.Workqueue' themselves — it's pure glue
-- between three effects that already have their own fakeable interpreters.
-- Making it an effect too would just add a layer of indirection with
-- nothing to dispatch on.
module Kubernetes.Operator.Reflector
  ( reflectorLoop
  ) where

import Control.Concurrent (threadDelay)
import Data.Aeson (FromJSON, parseJSON)
import Data.Aeson.Types (parseMaybe)
import Effectful
import Effectful.Exception (finally)
import Kubernetes.Client (Log, logInfo, logWarn)
import Kubernetes.Operator.Cache (Cache, cacheDelete, cacheReplace, cacheUpsert)
import Kubernetes.Operator.Client
  ( KubeClient
  , ListResult (..)
  , WatchEvent (..)
  , WatchEventType (..)
  , WatchHandle
  , closeWatch
  , listResources
  , nextEvent
  , openWatch
  )
import Kubernetes.Operator.Workqueue (Workqueue, wqAdd)
import Kubernetes.Resource (ObjectKey, Resource (resourceKey))
import Data.Text (Text)

-- | How long to wait before re-LISTing after the watch stream ends or
-- reports an error, so a persistently failing API server isn't hammered
-- with a tight LIST/watch/fail loop. A fixed delay is a deliberate
-- simplification; a production-grade Reflector would back off
-- exponentially here the same way 'Kubernetes.Operator.Workqueue.wqAddRateLimited'
-- does for reconciles.
resyncBackoffMicros :: Int
resyncBackoffMicros = 2 * 1000 * 1000

-- | Runs forever: LIST → replace the Cache wholesale → watch from that
-- LIST's resourceVersion, applying each event to the Cache and enqueuing
-- its key. On watch end (server closed the stream, e.g. after
-- @timeoutSeconds@) or an in-stream @ERROR@ event (typically
-- "resourceVersion too old" \/ 410 Gone), falls back to a fresh LIST rather
-- than trying to resume — the resync loop the base client's Haddock
-- explicitly left as future work.
--
-- Meant to run as its own thread (see
-- 'Kubernetes.Operator.Controller.compileController'); stopped by
-- cancelling that thread, not by any internal shutdown check. The 'finally'
-- around the watch loop guarantees 'closeWatch' runs even when cancellation
-- arrives as an asynchronous exception while blocked in 'nextEvent' — the
-- same pattern already exercised by the base client's Ctrl-C handling.
reflectorLoop
  :: forall a es
   . (Resource a, FromJSON a, KubeClient a :> es, Cache a :> es, Workqueue ObjectKey :> es, Log :> es, IOE :> es)
  => Eff es ()
reflectorLoop = resync
  where
    resync :: Eff es ()
    resync = do
      list <- listResources @a
      cacheReplace (lrItems list) (lrResourceVersion list)
      -- Prime the queue so a freshly (re)started controller reconciles
      -- every currently-known object once, not just future changes.
      mapM_ (wqAdd . resourceKey) (lrItems list)
      watch (lrResourceVersion list)

    -- Closes the handle (via 'finally') before deciding whether to resync,
    -- so we never have two watch connections open at once.
    watch :: Text -> Eff es ()
    watch rv = do
      wh <- openWatch rv
      needsResync <- watchLoop wh `finally` closeWatch wh
      if needsResync
        then do
          liftIO (threadDelay resyncBackoffMicros)
          resync
        else pure ()

    -- 'True' means the caller should close up and re-LIST.
    watchLoop :: WatchHandle a -> Eff es Bool
    watchLoop wh = do
      mEvt <- nextEvent wh
      case mEvt of
        Nothing -> do
          logInfo "watch ended by server; resyncing"
          pure True
        Just evt
          | weType evt == ErrorEvt -> do
              logInfo "watch ERROR event (commonly: resourceVersion too old / 410 Gone); resyncing"
              pure True
          | otherwise -> do
              applyEvent evt
              watchLoop wh

    applyEvent :: WatchEvent a -> Eff es ()
    applyEvent evt = case weType evt of
      -- A Bookmark's object normally carries only a fresh resourceVersion,
      -- not a full spec/status — upserting it would corrupt the cached
      -- object. Since disconnects always trigger a full re-LIST here rather
      -- than a resumed watch, there is nothing else to do with it.
      Bookmark -> pure ()
      Deleted -> case parseMaybe (parseJSON @a) (weObject evt) of
        Nothing -> logWarn "watch: failed to decode DELETED object; skipping"
        Just obj -> cacheDelete @a (resourceKey obj) >> wqAdd (resourceKey obj)
      _ -> case parseMaybe (parseJSON @a) (weObject evt) of
        Nothing -> logWarn "watch: failed to decode object; skipping"
        Just obj -> cacheUpsert obj >> wqAdd (resourceKey obj)
