{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

-- | The minimal read-only operator for a /cluster-scoped/ built-in kind.
-- Every other example watches a namespaced resource; this one watches Nodes,
-- where @namespaced = False@ in 'deriveResource' (so the 'Resource' instance
-- is cluster-scoped and the 'WatchScope' of the 'ControllerSpec' is ignored).
-- Logs each Node's readiness whenever its status changes.
--
-- This is the most compact of the worked examples: it uses 'deriveResource'
-- (Template Haskell) to generate the whole 'Resource' instance from one line
-- instead of hand-writing the five methods, and the 'done'/'requeueAfter'
-- helpers to spell a reconciler as plain prose. Compare @ConfigMapLogger.hs@
-- and @PodWatcher.hs@ to see the hand-written version of the same plumbing.
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

-- | The whole 'Resource' instance in one line: group ""/version "v1"/kind
-- "Node"/plural "nodes", cluster-scoped, metadata field @nodeMeta@. This
-- replaces the five hand-written methods below it that the other examples
-- spell out.
deriveResource ''Node "" "v1" "Node" "nodes" False 'nodeMeta

nodeReady :: Node -> Bool
nodeReady n =
  maybe False
    (any (\c -> nodeCondType c == "Ready" && nodeCondStatus c == "True") . nodeConditions)
    (nodeStatus n)

reconcileNode :: (Ctx Node es) => Request -> Maybe Node -> Eff es (Either ReconcileError ReconcileResult)
reconcileNode (Request key) mNode = do
  case mNode of
    Nothing -> logInfo (renderKey key <> " deleted") >> done
    Just n -> do
      logInfo (renderKey key <> " ready=" <> T.pack (show (nodeReady n)))
      done

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
          , csReconcile = onCached reconcileNode
          }

  metrics <- newMetricsRegistry
  controller <- compileController kubeConfig defaultOperatorConfig metrics spec
  withMetricsServer 9090 metrics (runManager defaultManagerConfig [controller])
