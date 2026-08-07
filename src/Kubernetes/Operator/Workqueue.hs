{-# LANGUAGE AllowAmbiguousTypes #-}

-- | A client-go-style rate-limited, deduplicating work queue, as a
-- first-order dynamic effect. This is the piece that turns a firehose of
-- watch events into a sane stream of "go look at this key" work items:
--
--   * Adding a key already queued is a no-op (deduplication).
--   * Adding a key that's currently being processed marks it /dirty/
--     instead of queuing a second, concurrent reconcile of the same
--     object; it's re-queued automatically when the in-flight one finishes
--     ('wqDone'). This is what makes it safe for a fast-moving watch to
--     never cause two workers to reconcile the same object at once.
--   * 'wqAddRateLimited' tracks a per-key failure counter and requeues
--     after an exponentially growing delay, capped at 'whMaxDelay'.
--
-- Entirely generic in the key type and has no notion of Kubernetes at all,
-- which is what makes it trivial to unit-test in isolation (no HTTP, no
-- fakes needed) — see roadmap step 3.
module Kubernetes.Operator.Workqueue
  ( Workqueue
  , wqAdd
  , wqAddAfter
  , wqAddRateLimited
  , wqForget
  , wqGet
  , wqDone
  , wqShutDown
  , wqLen
  , WorkqueueHandle
  , newWorkqueue
  , runWorkqueueIO
  , shutdownWorkqueueIO
  ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Monad (when)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Sequence (Seq, ViewL (..), (|>))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Time (NominalDiffTime)
import Effectful
import Effectful.Dispatch.Dynamic (interpret, send)

data Workqueue k :: Effect where
  WqAdd :: k -> Workqueue k m ()
  WqAddAfter :: k -> NominalDiffTime -> Workqueue k m ()
  WqAddRateLimited :: k -> Workqueue k m ()
  WqForget :: k -> Workqueue k m ()
  -- | Block until an item is ready, or the queue has been shut down and
  -- drained (in which case: 'Nothing'). Marks the item "processing" so a
  -- concurrent 'wqAdd' for the same key becomes a "dirty" re-queue instead
  -- of a second concurrent delivery.
  WqGet :: Workqueue k m (Maybe k)
  -- | Mark a key's processing finished; must be called exactly once per
  -- successful 'wqGet'. Re-queues the key if it was marked dirty meanwhile.
  WqDone :: k -> Workqueue k m ()
  WqShutDown :: Workqueue k m ()
  WqLen :: Workqueue k m Int

type instance DispatchOf (Workqueue k) = Dynamic

wqAdd :: (Workqueue k :> es) => k -> Eff es ()
wqAdd = send . WqAdd

wqAddAfter :: (Workqueue k :> es) => k -> NominalDiffTime -> Eff es ()
wqAddAfter k d = send (WqAddAfter k d)

wqAddRateLimited :: (Workqueue k :> es) => k -> Eff es ()
wqAddRateLimited = send . WqAddRateLimited

wqForget :: (Workqueue k :> es) => k -> Eff es ()
wqForget = send . WqForget

wqGet :: (Workqueue k :> es) => Eff es (Maybe k)
wqGet = send WqGet

wqDone :: (Workqueue k :> es) => k -> Eff es ()
wqDone = send . WqDone

wqShutDown :: forall k es. (Workqueue k :> es) => Eff es ()
wqShutDown = send (WqShutDown :: Workqueue k (Eff es) ())

wqLen :: forall k es. (Workqueue k :> es) => Eff es Int
wqLen = send (WqLen :: Workqueue k (Eff es) Int)

data WorkqueueHandle k = WorkqueueHandle
  { whQueue :: !(TVar (Seq k))
  -- ^ FIFO of keys ready to hand out via 'wqGet'.
  , whQueued :: !(TVar (Set k))
  -- ^ Mirrors the contents of 'whQueue', for O(log n) dedup checks.
  , whProcessing :: !(TVar (Set k))
  -- ^ Keys currently checked out (between 'wqGet' and 'wqDone').
  , whDirty :: !(TVar (Set k))
  -- ^ Keys added again while processing; re-queued on 'wqDone'.
  , whFailures :: !(TVar (Map k Int))
  -- ^ Per-key consecutive-failure counter driving 'wqAddRateLimited' backoff.
  , whShutdown :: !(TVar Bool)
  , whBaseDelay :: !NominalDiffTime
  , whMaxDelay :: !NominalDiffTime
  }

newWorkqueue :: NominalDiffTime -> NominalDiffTime -> IO (WorkqueueHandle k)
newWorkqueue baseDelay maxDelay =
  WorkqueueHandle
    <$> newTVarIO Seq.empty
    <*> newTVarIO Set.empty
    <*> newTVarIO Set.empty
    <*> newTVarIO Set.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO False
    <*> pure baseDelay
    <*> pure maxDelay

-- | Plain-'IO' escape hatch for triggering shutdown from outside an 'Eff'
-- computation entirely — e.g. 'Kubernetes.Operator.Controller.ccShutdown',
-- which the Manager calls from a signal handler. 'WqShutDown' is defined in
-- terms of this, not the other way around, so there is exactly one place
-- that flips the flag.
shutdownWorkqueueIO :: WorkqueueHandle k -> IO ()
shutdownWorkqueueIO wq = atomically (writeTVar (whShutdown wq) True)

-- | One 'WorkqueueHandle' (and hence one call to this) per Controller.
--
-- Delayed adds ('wqAddAfter', 'wqAddRateLimited') are implemented with one
-- 'forkIO' + 'threadDelay' per pending delay. That's simple and correct at
-- the scale of a single controller's retry traffic; if a future workload
-- needs many thousands of simultaneously-delayed keys, swap this for a
-- single timer-wheel thread without changing the effect's public API.
runWorkqueueIO :: forall k es r. (Ord k, IOE :> es) => WorkqueueHandle k -> Eff (Workqueue k : es) r -> Eff es r
runWorkqueueIO wq = interpret $ \_ -> \case
  WqAdd k -> liftIO (addNow k)
  WqAddAfter k delay -> liftIO (afterDelay delay (addNow k))
  WqAddRateLimited k -> liftIO $ do
    delay <- atomically $ do
      n <- Map.findWithDefault 0 k <$> readTVar (whFailures wq)
      modifyTVar' (whFailures wq) (Map.insert k (n + 1))
      pure (min (whMaxDelay wq) (whBaseDelay wq * (2 ^ n)))
    afterDelay delay (addNow k)
  WqForget k -> liftIO . atomically $ modifyTVar' (whFailures wq) (Map.delete k)
  WqGet -> liftIO . atomically $ do
    down <- readTVar (whShutdown wq)
    q <- readTVar (whQueue wq)
    case Seq.viewl q of
      EmptyL
        | down -> pure Nothing
        | otherwise -> retry
      k :< rest -> do
        writeTVar (whQueue wq) rest
        modifyTVar' (whQueued wq) (Set.delete k)
        modifyTVar' (whProcessing wq) (Set.insert k)
        pure (Just k)
  WqDone k -> liftIO . atomically $ do
    modifyTVar' (whProcessing wq) (Set.delete k)
    dirty <- readTVar (whDirty wq)
    when (Set.member k dirty) $ do
      modifyTVar' (whDirty wq) (Set.delete k)
      enqueue k
  WqShutDown -> liftIO (shutdownWorkqueueIO wq)
  WqLen -> liftIO (Seq.length <$> readTVarIO (whQueue wq))
  where
    addNow :: k -> IO ()
    addNow k = atomically $ do
      processing <- readTVar (whProcessing wq)
      if Set.member k processing
        then modifyTVar' (whDirty wq) (Set.insert k)
        else enqueue k

    enqueue :: k -> STM ()
    enqueue k = do
      queued <- readTVar (whQueued wq)
      when (not (Set.member k queued)) $ do
        modifyTVar' (whQueue wq) (|> k)
        modifyTVar' (whQueued wq) (Set.insert k)

    afterDelay :: NominalDiffTime -> IO () -> IO ()
    afterDelay delay act = do
      _ <- forkIO (threadDelay (max 0 (round (realToFrac delay * 1000000 :: Double))) >> act)
      pure ()
