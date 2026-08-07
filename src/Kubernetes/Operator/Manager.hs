-- | Runs one or more compiled controllers and handles graceful shutdown.
--
-- Deliberately plain 'IO', not 'Eff': by the time a 'CompiledController'
-- exists, all of its effects have already been interpreted down to 'IO' by
-- 'Kubernetes.Operator.Controller.compileController'. The Manager's job —
-- supervise N independent 'IO' actions and propagate cancellation between
-- them — has nothing to do with which effects those actions used
-- internally, so reaching for plain "async" is the right tool, not a
-- concession. This is also what lets 'runManager' run controllers for
-- entirely different resource kinds (different effect rows) side by side
-- without any existential wrapper: they're all just @IO ()@ by this point.
module Kubernetes.Operator.Manager
  ( ManagerConfig (..)
  , defaultManagerConfig
  , runManager
  ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async)
import qualified Control.Concurrent.Async as Async
import Control.Exception (SomeException, try)
import Control.Monad (forM_)
import Data.Time (NominalDiffTime)
import Kubernetes.Operator.Controller (CompiledController (..))
import Kubernetes.Operator.Internal.Async (withAsyncs)
import System.IO (hPutStrLn, stderr)
import System.Posix.Signals (Handler (Catch), installHandler, sigTERM)

data ManagerConfig = ManagerConfig
  { mcShutdownGracePeriod :: !NominalDiffTime
  -- ^ How long to let in-flight reconciles and Reflector watch connections
  -- close cleanly after a shutdown is requested before force-cancelling.
  , mcInstallSigTermHandler :: !Bool
  -- ^ Kubernetes stops Pods with SIGTERM, not Ctrl-C's SIGINT (which GHC's
  -- RTS already turns into a normal, 'Effectful.Exception.finally'-visible
  -- 'Control.Exception.UserInterrupt', as used by the base client). Install
  -- a handler that routes SIGTERM through the same graceful-shutdown path.
  -- POSIX-only, which is fine — this framework targets Pods, not Windows.
  }

defaultManagerConfig :: ManagerConfig
defaultManagerConfig =
  ManagerConfig
    { mcShutdownGracePeriod = 30
    , mcInstallSigTermHandler = True
    }

-- | Run every controller concurrently until one of two things happens:
--
--   * a controller's 'ccRun' returns or throws on its own (e.g. an
--     unrecoverable HTTP failure) — treated as fatal for the whole Manager,
--     "let it crash" style: Kubernetes will restart the Pod and every
--     controller resumes from a fresh LIST, which is exactly the recovery
--     path they're built to support; or
--   * shutdown is requested (Ctrl-C / SIGTERM).
--
-- Either way, every controller is asked to stop gracefully
-- ('ccShutdown'), given 'mcShutdownGracePeriod' to finish in-flight
-- reconciles and close watch connections, and anything still running past
-- that deadline is force-cancelled.
runManager :: ManagerConfig -> [CompiledController] -> IO ()
runManager cfg ctrls = do
  let shutdownAll = forM_ ctrls ccShutdown
  installSigTermHandler cfg shutdownAll
  withAsyncs (map ccRun ctrls) $ \asyncs -> do
    outcome <- try (Async.waitAny asyncs) :: IO (Either SomeException (Async (), ()))
    case outcome of
      Left e -> hPutStrLn stderr ("Manager: a controller failed: " <> show e)
      Right _ -> pure ()

    shutdownAll
    graceResult <-
      Async.race
        (mapM_ Async.wait asyncs)
        (threadDelay (microsFor (mcShutdownGracePeriod cfg)))
    case graceResult of
      Left () -> pure () -- everyone stopped cleanly within the grace period
      Right () -> do
        hPutStrLn stderr "Manager: shutdown grace period elapsed, cancelling remaining controllers"
        mapM_ Async.cancel asyncs
  where
    microsFor :: NominalDiffTime -> Int
    microsFor d = max 0 (round (realToFrac d * 1000000 :: Double))

installSigTermHandler :: ManagerConfig -> IO () -> IO ()
installSigTermHandler cfg shutdownAll
  | not (mcInstallSigTermHandler cfg) = pure ()
  | otherwise = () <$ installHandler sigTERM (Catch shutdownAll) Nothing
