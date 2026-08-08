{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

-- | The floor: the whole framework works identically for a built-in kind,
-- not just CRDs — a 'Resource' instance is all a kind ever needs, whether
-- it's something @crd-codegen@ produced, a CRD you typed by hand, or (as
-- here) Pod itself. Logs each Pod's @status.phase@ whenever it changes.
--
-- This intentionally decodes only @metadata@ and @status.phase@ — a real
-- Pod has dozens of fields; a 'Resource' instance only needs to model the
-- ones your reconciler actually reads.
module Main (main) where

import Data.Aeson (FromJSON (..), withObject, (.:), (.:?))
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Effectful (Eff)
import Kubernetes.Operator
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

data Pod = Pod
  { podMeta :: ObjectMeta
  , podPhase :: Maybe Text
  }

instance FromJSON Pod where
  parseJSON = withObject "Pod" $ \o -> do
    meta <- o .: "metadata"
    status <- o .:? "status"
    phase <- maybe (pure Nothing) (.:? "phase") status
    pure (Pod meta phase)

-- | The whole 'Resource' instance in one line (group ""/version
-- "v1"/kind "Pod"/plural "pods", namespaced, metadata field @podMeta@).
deriveResource ''Pod "" "v1" "Pod" "pods" True 'podMeta

reconcilePod :: (Ctx Pod es) => Request -> Maybe Pod -> Eff es (Either ReconcileError ReconcileResult)
reconcilePod (Request key) mPod = do
  case mPod of
    Nothing -> logInfo (renderKey key <> " deleted") >> done
    Just pod -> do
      logInfo (renderKey key <> " phase=" <> fromMaybe "<unknown>" (podPhase pod))
      done

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  kubeConfig <- loadKubeConfig
  let scope = case kcNamespace kubeConfig of
        AllNamespaces -> WatchAllNamespaces
        NS ns -> WatchNamespace ns

      spec :: ControllerSpec Pod
      spec =
        ControllerSpec
          { csName = "pod-watcher"
          , csScope = scope
          , csWorkers = 2
          , csMaxRetries = 3
          , csReconcile = onCached reconcilePod
          }

  metrics <- newMetricsRegistry
  controller <- compileController kubeConfig defaultOperatorConfig metrics spec
  withMetricsServer 9090 metrics (runManager defaultManagerConfig [controller])
