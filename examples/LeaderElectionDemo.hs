{-# LANGUAGE OverloadedStrings #-}

-- | Demonstrates "Kubernetes.Operator.LeaderElection" in isolation: prints
-- when it becomes and stops being leader. Run two copies with different
-- identities against the same cluster/namespace/lease name to see only
-- one become leader, and the other take over once the first is killed.
--
-- > IDENTITY=a cabal run leader-election-demo
-- > IDENTITY=b cabal run leader-election-demo   -- in another terminal
module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Monad (forever)
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Kubernetes.Client (loadKubeConfig)
import Kubernetes.Operator.LeaderElection (defaultLeaderElectionConfig, runWithLeaderElection)
import System.Environment (lookupEnv)
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  identity <- T.pack . fromMaybe "unknown" <$> lookupEnv "IDENTITY"
  kubeConfig <- loadKubeConfig
  let lec = defaultLeaderElectionConfig "demo-leader" "default" identity
  putStrLn (T.unpack identity <> ": starting, trying to acquire leadership...")
  runWithLeaderElection
    kubeConfig
    lec
    ( do
        putStrLn (T.unpack identity <> ": ***** became leader *****")
        forever (threadDelay 1000000)
    )
    (putStrLn (T.unpack identity <> ": stopped leading"))
