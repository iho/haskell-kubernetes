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
  , reflectorLoopFor
  ) where

import Control.Concurrent (threadDelay)
import Data.Aeson (FromJSON, parseJSON)
import Data.Aeson.Types (parseMaybe)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Effectful
import Effectful.Concurrent (Concurrent)
import Effectful.Concurrent.Async (race)
import Effectful.Exception (finally)
import Effectful.Reader.Static (Reader, asks)
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
import Kubernetes.Operator.Types (OperatorConfig (..))
import Kubernetes.Operator.Workqueue (Workqueue, wqAdd)
import Kubernetes.Resource (ObjectKey, Resource (resourceKey))

-- | How long to wait before re-LISTing after the watch stream ends or
-- reports an error, so a persistently failing API server isn't hammered
-- with a tight LIST/watch/fail loop. A fixed delay is a deliberate
-- simplification; a production-grade Reflector would back off
-- exponentially here the same way 'Kubernetes.Operator.Workqueue.wqAddRateLimited'
-- does for reconciles.
resyncBackoffMicros :: Int
resyncBackoffMicros = 2 * 1000 * 1000

microsFor :: NominalDiffTime -> Int
microsFor d = max 0 (round (realToFrac d * 1000000 :: Double))

-- | Runs forever: LIST → replace the Cache wholesale → watch from that
-- LIST's resourceVersion, applying each event to the Cache and enqueuing
-- its key. On watch end (server closed the stream, e.g. after
-- @timeoutSeconds@) or an in-stream @ERROR@ event (typically
-- "resourceVersion too old" / 410 Gone), falls back to a fresh LIST rather
-- than trying to resume. The watch is additionally time-limited to
-- 'Kubernetes.Operator.Types.ocResyncPeriod': if that much wall-clock time
-- passes with no terminating event, the reflector re-LISTs anyway, so a
-- watch notification the client silently dropped is healed by the next
-- periodic resync even though the stream itself never ended. This is what
-- makes 'Kubernetes.Operator.Types.ocResyncPeriod' actually meaningful —
-- previously it was declared but never consulted.
--
-- Meant to run as its own thread (see
-- 'Kubernetes.Operator.Controller.compileController'); stopped by
-- cancelling that thread, not by any internal shutdown check. The 'finally'
-- around the watch loop guarantees 'closeWatch' runs even when cancellation
-- arrives as an asynchronous exception while blocked in 'nextEvent' — the
-- same pattern already exercised by the base client's Ctrl-C handling.
reflectorLoop
  :: forall a es
   . (Resource a, FromJSON a, KubeClient a :> es, Cache a :> es, Workqueue ObjectKey :> es, Log :> es, Reader OperatorConfig :> es, Concurrent :> es, IOE :> es)
  => Eff es ()
reflectorLoop = reflectorLoopFor @a (pure . resourceKey)

-- | As 'reflectorLoop', but with control over which key(s) a changed or
-- deleted object of kind @a@ enqueues, instead of always its own
-- 'resourceKey'. This is the hook a /secondary/ watch uses to translate a
-- child object into its controlling owner's key — see
-- 'Kubernetes.Operator.OwnerReference.controllingOwnerKey' for the typical
-- @a -> ['ObjectKey']@ passed here, and
-- 'Kubernetes.Operator.Controller.watchOwnedBy' for the combinator that
-- builds one. The local Cache is still kept in sync for kind @a@ exactly as
-- 'reflectorLoop' does; only what gets enqueued (and on whose Workqueue,
-- determined by which interpreter @es@ carries) differs.
reflectorLoopFor
  :: forall a es
   . (Resource a, FromJSON a, KubeClient a :> es, Cache a :> es, Workqueue ObjectKey :> es, Log :> es, Reader OperatorConfig :> es, Concurrent :> es, IOE :> es)
  => (a -> [ObjectKey])
  -> Eff es ()
reflectorLoopFor keysFor = resync
  where
    resync :: Eff es ()
    resync = do
      list <- listResources @a
      cacheReplace (lrItems list) (lrResourceVersion list)
      -- Prime the queue so a freshly (re)started controller reconciles
      -- every currently-known object once, not just future changes.
      mapM_ (mapM_ wqAdd . keysFor) (lrItems list)
      watch (lrResourceVersion list)

    -- Closes the handle (via 'finally') before deciding whether to resync,
    -- so we never have two watch connections open at once.
    watch :: Text -> Eff es ()
    watch rv = do
      wh <- openWatch rv
      needsResync <- watchLoopPeriod wh `finally` closeWatch wh
      if needsResync
        then do
          liftIO (threadDelay resyncBackoffMicros)
          resync
        else pure ()

    -- Watch for up to 'ocResyncPeriod' of wall-clock time before forcing a
    -- resync. 'True' means the caller should close up and re-LIST — either
    -- because the stream ended (server close or @ERROR@ event) or because
    -- the resync period elapsed while we were still watching.
    watchLoopPeriod :: WatchHandle a -> Eff es Bool
    watchLoopPeriod wh = do
      periodMicros <- asks (microsFor . ocResyncPeriod)
      res <- race (watchLoop wh) (liftIO (threadDelay periodMicros))
      case res of
        Left stopNow -> pure stopNow
        Right () -> do
          logInfo "resync period elapsed; forcing re-LIST"
          pure True
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
        Just obj -> cacheDelete @a (resourceKey obj) >> mapM_ wqAdd (keysFor obj)
      _ -> case parseMaybe (parseJSON @a) (weObject evt) of
        Nothing -> logWarn "watch: failed to decode object; skipping"
        Just obj -> cacheUpsert obj >> mapM_ wqAdd (keysFor obj)
