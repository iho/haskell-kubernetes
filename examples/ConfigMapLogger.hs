{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

-- | A minimal, real operator built on "Kubernetes.Operator": watches
-- ConfigMaps (cluster-wide or in one namespace, per the usual
-- @KUBE_NAMESPACE@\/@KUBE_ALL_NAMESPACES@ env vars) and logs each one's
-- data keys whenever it's added, changed, or removed.
--
-- This is the example from the design write-up, now actually buildable:
--
-- > cabal run configmap-logger
--
-- against a @kubectl proxy@, or with @KUBE_API_SERVER@\/@KUBE_TOKEN@\/
-- @KUBE_CA_FILE@ set against a real cluster — see the top-level README.
module Main (main) where

import Data.Aeson (FromJSON (..), withObject, (.:), (.:?))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Effectful (Eff)
import Kubernetes.Operator
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

-- | Everything the generic machinery needs to know about a ConfigMap: its
-- identity ('ObjectMeta', reused as-is) plus the one field this reconciler
-- actually cares about.
data ConfigMap = ConfigMap
  { cmMeta :: ObjectMeta
  , cmData :: Map Text Text
  }
  deriving (Show)

instance FromJSON ConfigMap where
  parseJSON = withObject "ConfigMap" $ \o ->
    ConfigMap
      <$> o .: "metadata"
      <*> (fromMaybe Map.empty <$> o .:? "data")

-- | The only bit of plumbing a new Kind needs: where it lives (GVK, REST
-- plural, namespaced-or-not) and how to get its identity out of a value.
-- Nothing else in "Kubernetes.Operator" is Pod- or ConfigMap-specific.
-- Here the whole instance is generated from one line by 'deriveResource'.
deriveResource ''ConfigMap "" "v1" "ConfigMap" "configmaps" True 'cmMeta

-- | The whole reconciler: 'onCached' has already looked the object up by key
-- (never trust an event payload — see 'Request''s Haddock for why), so this
-- just logs what's there now, or that it's gone. Real reconcilers would also
-- *act* on the state (create a companion resource, call out to something)
-- but the shape — read current state from the Cache, decide, report a
-- 'ReconcileResult' — is the same.
reconcileConfigMap :: (Ctx ConfigMap es) => Request -> Maybe ConfigMap -> Eff es (Either ReconcileError ReconcileResult)
reconcileConfigMap (Request key) mCm = do
  case mCm of
    Nothing -> do
      logInfo ("configmap " <> renderKey key <> " deleted")
      done
    Just cm -> do
      logInfo
        ( "configmap "
            <> renderKey key
            <> " keys: "
            <> T.pack (show (Map.keys (cmData cm)))
        )
      done

main :: IO ()
main = do
  -- Container runtimes capture stdout as a pipe, not a TTY, so GHC defaults
  -- to full block buffering — log lines would otherwise sit unflushed
  -- indefinitely in a long-running controller. Without this, `kubectl
  -- logs` on a real deployment shows nothing at all.
  hSetBuffering stdout LineBuffering
  kubeConfig <- loadKubeConfig
  let scope = case kcNamespace kubeConfig of
        AllNamespaces -> WatchAllNamespaces
        NS ns -> WatchNamespace ns

      spec :: ControllerSpec ConfigMap
      spec =
        ControllerSpec
          { csName = "configmap-logger"
          , csScope = scope
          , csWorkers = 4
          , csMaxRetries = 5
          , csSecondaryWatches = []
          , csReconcile = onCached reconcileConfigMap
          }

  metrics <- newMetricsRegistry
  controller <- compileController kubeConfig defaultOperatorConfig metrics spec
  withMetricsServer 9090 metrics (runManager defaultManagerConfig [controller])
