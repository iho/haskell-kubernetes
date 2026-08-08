-- | Entry point for the Pod list+watch demo: load cluster config
-- ('loadKubeConfig' — in-cluster ServiceAccount, explicit token + CA, or a
-- plain @kubectl proxy@), build an http-client 'Manager' accordingly, then
-- hand off to 'Kubernetes.Client.program'.
--
-- Nothing in here is effectful-specific: this module is the IO shell around
-- the effect stack, kept separate from the effect logic in
-- "Kubernetes.Client" on purpose.
module Main (main) where

import Control.Exception (AsyncException (UserInterrupt), catch, throwIO, try)
import Effectful (runEff)
import Kubernetes.Client
import System.Exit (exitFailure)
import System.IO (BufferMode (LineBuffering), hPutStrLn, hSetBuffering, stderr, stdout)

main :: IO ()
main = run `catch` handleInterrupt
  where
    run = do
      -- Container runtimes capture stdout as a pipe, not a TTY, so GHC
      -- defaults to full block buffering — log lines would otherwise sit
      -- unflushed indefinitely in a long-running watch loop. Without this,
      -- `kubectl logs` on a real deployment shows nothing at all.
      hSetBuffering stdout LineBuffering
      cfg <- loadKubeConfig
      mgr <- newManagerFor cfg
      result <- try (runEff . runLogIO $ runKubeClientIO mgr cfg program)
      case result of
        Right () -> pure ()
        Left err -> handleFailure err

    handleFailure :: KubeApiError -> IO ()
    handleFailure err@(KubeApiError 410 _) =
      putStrLn ("Watch expired (410 Gone): " <> show err <> ". Exiting cleanly (re-list on expiry is left as a future improvement).")
    handleFailure err = do
      hPutStrLn stderr ("Fatal: " <> show err)
      exitFailure

    handleInterrupt :: AsyncException -> IO ()
    handleInterrupt UserInterrupt = putStrLn "Interrupted by user, shutting down."
    handleInterrupt e = throwIO e
