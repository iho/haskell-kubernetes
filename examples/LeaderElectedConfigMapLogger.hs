{-# LANGUAGE OverloadedStrings #-}

-- | The production pattern for running more than one replica of an
-- operator: wrap 'runManager' in 'runWithLeaderElection' so only one
-- replica actually reconciles at a time, while the others sit warm ready
-- to take over the instant the active one disappears. Everything else is
-- exactly @ConfigMapLogger.hs@ — leader election is a wrapper around the
-- entry point, not something reconcile logic needs to know about at all.
--
-- Run two copies against the same cluster\/namespace with different
-- identities to see it in action, the same way as @LeaderElectionDemo.hs@:
--
-- > IDENTITY=a cabal run leader-elected-configmap-logger
-- > IDENTITY=b cabal run leader-elected-configmap-logger   -- another terminal
--
-- In a real Deployment, set @IDENTITY@ from the downward API
-- (@fieldRef: metadata.name@) so each Pod gets its own.
module Main (main) where

import Data.Aeson (FromJSON (..), withObject, (.:), (.:?))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Effectful (Eff)
import Kubernetes.Operator
import System.Environment (lookupEnv)
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

data ConfigMap = ConfigMap
  { cmMeta :: ObjectMeta
  , cmData :: Map Text Text
  }

instance FromJSON ConfigMap where
  parseJSON = withObject "ConfigMap" $ \o ->
    ConfigMap <$> o .: "metadata" <*> (fromMaybe Map.empty <$> o .:? "data")

instance Resource ConfigMap where
  resourceGVK _ = GVK "" "v1" "ConfigMap"
  resourceScope _ = Namespaced
  resourcePlural _ = "configmaps"
  resourceMeta = cmMeta
  resourceSetMeta m cm = cm {cmMeta = m}

reconcileConfigMap :: (Ctx ConfigMap es) => Request -> Eff es (Either ReconcileError ReconcileResult)
reconcileConfigMap (Request key) = do
  mCm <- cacheGet @ConfigMap key
  case mCm of
    Nothing -> logInfo ("configmap " <> renderKey key <> " deleted") >> pure (Right Done)
    Just cm -> do
      logInfo ("configmap " <> renderKey key <> " keys: " <> T.pack (show (Map.keys (cmData cm))))
      pure (Right Done)

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  kubeConfig <- loadKubeConfig
  identity <- T.pack . fromMaybe "local" <$> (lookupEnv "IDENTITY" `orElseEnv` "POD_NAME")

  let scope = case kcNamespace kubeConfig of
        AllNamespaces -> WatchAllNamespaces
        NS ns -> WatchNamespace ns

      spec :: ControllerSpec ConfigMap
      spec =
        ControllerSpec
          { csName = "leader-elected-configmap-logger"
          , csScope = scope
          , csWorkers = 4
          , csMaxRetries = 5
          , csReconcile = reconcileConfigMap
          }

      leaderNamespace = case kcNamespace kubeConfig of
        NS ns -> ns
        AllNamespaces -> "default" -- Leases are namespaced; pick one to coordinate in regardless of watch scope.
      lec = defaultLeaderElectionConfig "configmap-logger-leader" leaderNamespace identity

  -- Served for the whole process lifetime, not just while leading: a
  -- standby replica is still alive and worth scraping (e.g. to alert if
  -- *no* replica has been leader for too long), the same way a real HA
  -- operator's /metrics and /healthz stay up regardless of leader status.
  metrics <- newMetricsRegistry
  controller <- compileController kubeConfig defaultOperatorConfig metrics spec

  putStrLn (T.unpack identity <> ": trying to acquire leadership...")
  withMetricsServer 9090 metrics $
    runWithLeaderElection
      kubeConfig
      lec
      ( do
          putStrLn (T.unpack identity <> ": elected leader, starting controller")
          runManager defaultManagerConfig [controller]
      )
      (putStrLn (T.unpack identity <> ": lost leadership, controller stopped"))
  where
    orElseEnv :: IO (Maybe String) -> String -> IO (Maybe String)
    orElseEnv first fallbackName = do
      mv <- first
      case mv of
        Just _ -> pure mv
        Nothing -> lookupEnv fallbackName
