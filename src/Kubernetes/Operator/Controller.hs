{-# LANGUAGE RankNTypes #-}

-- | A Controller wires one Reconciler to one resource kind's Cache,
-- Workqueue, and Reflector.
module Kubernetes.Operator.Controller
  ( Ctx
  , ControllerSpec (..)
  , CompiledController (..)
  , workerLoop
  , compileController
  ) where

import qualified Control.Concurrent.Async as Async
import qualified Control.Exception as IOException
import Data.Aeson (FromJSON)
import Data.Text (Text)
import qualified Data.Text as T
import Effectful
import Effectful.Exception (SomeException, try)
import Effectful.Reader.Static (Reader, runReader)
import Kubernetes.Client (KubeConfig, Log, logErr, newManagerFor, runLogIO)
import Kubernetes.Operator.Cache (Cache, CacheStore, newCacheStore, runCacheIO)
import Kubernetes.Operator.Client (KubeClient, runKubeClientIO)
import Kubernetes.Operator.Internal.Async (withAsyncs)
import Kubernetes.Operator.Reflector (reflectorLoop)
import Kubernetes.Operator.Types
  ( OperatorConfig
  , ReconcileError (..)
  , ReconcileResult (..)
  , Request (..)
  )
import Kubernetes.Resource (ObjectKey, Resource, WatchScope, renderKey)
import Kubernetes.Operator.Workqueue
  ( Workqueue
  , WorkqueueHandle
  , newWorkqueue
  , runWorkqueueIO
  , shutdownWorkqueueIO
  , wqAdd
  , wqAddAfter
  , wqAddRateLimited
  , wqDone
  , wqForget
  , wqGet
  )

-- | The capability row a reconciler needs — the concrete stand-in for
-- "Context". Rather than threading a bespoke @Context@ value by hand, we
-- express it as a constraint on the ambient effect row: adding a new
-- capability later (say, a @Metrics@ effect) is one line here, and every
-- reconciler that doesn't need it is unaffected. 'Log' and 'KubeConfig' are
-- reused as-is from the already-verified 'Kubernetes.Client'; 'KubeClient'
-- here is the generalized effect from 'Kubernetes.Operator.Client'.
type Ctx a es =
  ( Resource a
  , KubeClient a :> es
  , Cache a :> es
  , Workqueue ObjectKey :> es
  , Log :> es
  , Reader OperatorConfig :> es
  , IOE :> es
  )

-- | A reconciler plus the handful of knobs a controller needs around it.
-- 'csReconcile' is rank-2 ('forall es. Ctx a es => ...') so it can be
-- called from inside 'compileController' once the concrete @es@ (fixed by
-- which interpreters get stacked there) is known, while remaining
-- effect-agnostic at definition time — a reconciler author never has to
-- name a concrete stack, only the capabilities they use.
data ControllerSpec a = ControllerSpec
  { csName :: !Text
  , csScope :: !WatchScope
  -- ^ Which namespace(s) this controller watches; see 'WatchScope''s
  -- Haddock for why this isn't folded into 'KubeConfig'.
  , csWorkers :: !Int
  , csMaxRetries :: !Int
  -- ^ Currently advisory (see 'workerLoop' Haddock); wiring this into a
  -- give-up-after-N-failures policy is a small addition to 'handleOutcome'.
  , csReconcile :: forall es. Ctx a es => Request -> Eff es (Either ReconcileError ReconcileResult)
  }

-- | A controller with all of its effects already interpreted down to 'IO'.
-- 'ccRun' starts the reflector and worker pool and blocks until stopped;
-- 'ccShutdown' requests a graceful stop and is safe to call more than once
-- or concurrently with 'ccRun' (see 'Kubernetes.Operator.Manager').
data CompiledController = CompiledController
  { ccName :: !Text
  , ccRun :: IO ()
  , ccShutdown :: IO ()
  }

-- | One worker: pull a key, reconcile it, act on the result, repeat until
-- the workqueue is shut down and drained. This is the part of a Controller
-- that doesn't depend on the (not-yet-generalized) HTTP client, so unlike
-- 'compileController' it's implemented for real and is usable today against
-- a hand-rolled test interpreter for @KubeClient a@.
--
-- Design decision: an exception escaping 'csReconcile' is caught here and
-- turned into a 'TransientError' rather than killing the worker thread. A
-- single reconciler bug or an unexpected exception (e.g. from a JSON decode
-- deep in a helper) should degrade to "retry with backoff", not take down
-- the whole controller. 'Either'/'ReconcileError' stays the primary,
-- *expected* control-flow surface reconciler authors write against; this
-- 'try' is a safety net at the worker boundary, not a substitute for it.
workerLoop :: forall a es. Ctx a es => ControllerSpec a -> Eff es ()
workerLoop spec = loop
  where
    loop :: Eff es ()
    loop = do
      mKey <- wqGet
      case mKey of
        Nothing -> pure () -- queue shut down and drained: exit cleanly
        Just key -> do
          outcome <- reconcileSafely key
          handleOutcome key outcome
          loop

    reconcileSafely :: ObjectKey -> Eff es (Either ReconcileError ReconcileResult)
    reconcileSafely key = do
      r <- try (csReconcile spec (Request key))
      pure $ case r of
        Right res -> res
        Left (e :: SomeException) -> Left (TransientError (T.pack (show e)))

    handleOutcome :: ObjectKey -> Either ReconcileError ReconcileResult -> Eff es ()
    handleOutcome key = \case
      Right Done -> wqForget key >> wqDone key
      Right (RequeueAfter d) -> wqDone key >> wqAddAfter key d
      Right RequeueImmediately -> wqDone key >> wqAdd key
      Left (PermanentError msg) -> do
        logErr ("reconcile " <> renderKey key <> " permanently failed: " <> msg)
        wqForget key >> wqDone key
      Left (TransientError msg) -> do
        logErr ("reconcile " <> renderKey key <> " failed, retrying: " <> msg)
        wqDone key >> wqAddRateLimited key

-- | Wire a 'ControllerSpec' up to a real, running controller: its own
-- 'Kubernetes.Operator.Cache.CacheStore', its own
-- 'Kubernetes.Operator.Workqueue.WorkqueueHandle', a
-- 'Kubernetes.Operator.Reflector.reflectorLoop' thread keeping both in sync
-- with the API server, and a pool of 'csWorkers' worker threads running
-- 'workerLoop'.
--
-- Concurrency: the reflector and every worker run as siblings under nested
-- 'Control.Concurrent.Async.withAsync' scopes ('withAsyncs'), so cancelling
-- or leaving this function's returned 'ccRun' action tears every one of
-- them down automatically — no manual bookkeeping of which threads exist.
-- If the reflector dies (an unrecoverable HTTP error) or any worker throws
-- an exception 'workerLoop' didn't already turn into a 'TransientError',
-- that's treated as fatal for the whole controller and re-thrown from
-- 'ccRun', consistent with 'Kubernetes.Operator.Manager.runManager''s
-- "let it crash" handling. The graceful path — 'ccShutdown' is called —
-- makes every 'wqGet' return 'Nothing' once drained, all workers return
-- normally, and only then is the (otherwise-immortal) reflector cancelled.
--
-- Requires 'Effectful.ConcUnlift' with 'Effectful.Persistent' \/
-- 'Effectful.Unlimited': the reflector and every worker unlift the /same/
-- 'Eff' environment onto their own thread concurrently, which is exactly
-- what that strategy (and only that strategy) supports — see the
-- accompanying design notes' concurrency section.
compileController :: forall a. (Resource a, FromJSON a) => KubeConfig -> OperatorConfig -> ControllerSpec a -> IO CompiledController
compileController kubeConfig opConfig spec = do
  mgr <- newManagerFor kubeConfig
  cacheStore <- newCacheStore :: IO (CacheStore a)
  wq <- newWorkqueue 1 60 :: IO (WorkqueueHandle ObjectKey) -- 1s base / 60s max reconcile backoff; not yet exposed via OperatorConfig
  let go :: forall es. (FromJSON a, Ctx a es) => Eff es ()
      go = withEffToIO (ConcUnlift Persistent Unlimited) $ \runInIO ->
        withAsyncs (runInIO (reflectorLoop @a) : replicate (csWorkers spec) (runInIO (workerLoop spec))) $ \case
          [] -> pure () -- unreachable: the reflector is always present
          (reflAsync : workerAsyncs) -> do
            outcome <- Async.race (Async.waitCatch reflAsync) (mapM Async.waitCatch workerAsyncs)
            case outcome of
              Left reflResult -> either IOException.throwIO pure reflResult
              Right workerResults -> mapM_ (either IOException.throwIO pure) workerResults

      run =
        runEff
          . runReader opConfig
          . runLogIO
          . runWorkqueueIO wq
          . runCacheIO cacheStore
          . runKubeClientIO @a mgr kubeConfig (csScope spec)
          $ go
  pure
    CompiledController
      { ccName = csName spec
      , ccRun = run
      , ccShutdown = shutdownWorkqueueIO wq
      }
