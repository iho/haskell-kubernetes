{-# LANGUAGE OverloadedStrings #-}

-- | The minimal read-only operator for a /cluster-scoped/ built-in kind.
-- Every other example watches a namespaced resource; this one watches Nodes,
-- where 'resourceScope _ = ClusterScoped' and the 'WatchScope' of the
-- 'ControllerSpec' is ignored (there is no namespace to scope by). Logs each
-- Node's readiness whenever its status changes.
--
-- It intentionally decodes only @metadata@ and @status.conditions@ — a real
-- Node has dozens of fields; a 'Resource' instance only needs to model the
-- ones your reconciler actually reads.
--
-- > cabal run node-watcher
module Main (main) where

import Data.Aeson (FromJSON (..), withObject, (.:), (.:?))
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Effectful (Eff)
import Kubernetes.Operator
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

data NodeCondition = NodeCondition
  { nodeCondType :: Text
  , nodeCondStatus :: Text
  }

instance FromJSON NodeCondition where
  parseJSON = withObject "NodeCondition" $ \o ->
    NodeCondition <$> o .: "type" <*> o .: "status"

data NodeStatus = NodeStatus
  { nodeConditions :: [NodeCondition]
  }

instance FromJSON NodeStatus where
  parseJSON = withObject "NodeStatus" $ \o ->
    NodeStatus <$> (fromMaybe [] <$> o .:? "conditions")

data Node = Node
  { nodeMeta :: ObjectMeta
  , nodeStatus :: Maybe NodeStatus
  }

instance FromJSON Node where
  parseJSON = withObject "Node" $ \o ->
    Node <$> o .: "metadata" <*> o .:? "status"

instance Resource Node where
  resourceGVK _ = GVK "" "v1" "Node"
  resourceScope _ = ClusterScoped
  resourcePlural _ = "nodes"
  resourceMeta = nodeMeta
  resourceSetMeta m n = n {nodeMeta = m}

nodeReady :: Node -> Bool
nodeReady n =
  maybe False
    (any (\c -> nodeCondType c == "Ready" && nodeCondStatus c == "True") . nodeConditions)
    (nodeStatus n)

reconcileNode :: (Ctx Node es) => Request -> Eff es (Either ReconcileError ReconcileResult)
reconcileNode (Request key) = do
  mNode <- cacheGet @Node key
  case mNode of
    Nothing -> logInfo (renderKey key <> " deleted") >> pure (Right Done)
    Just n -> do
      logInfo (renderKey key <> " ready=" <> T.pack (show (nodeReady n)))
      pure (Right Done)

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  kubeConfig <- loadKubeConfig
  let -- ClusterScoped: the WatchScope is ignored, so the choice is cosmetic;
      -- keep it explicit for readability.
      spec :: ControllerSpec Node
      spec =
        ControllerSpec
          { csName = "node-watcher"
          , csScope = WatchAllNamespaces
          , csWorkers = 2
          , csMaxRetries = 3
          , csReconcile = reconcileNode
          }

  metrics <- newMetricsRegistry
  controller <- compileController kubeConfig defaultOperatorConfig metrics spec
  withMetricsServer 9090 metrics (runManager defaultManagerConfig [controller])
