{-# LANGUAGE OverloadedStrings #-}

-- | The minimal /write/ operator: for every ConfigMap carrying the data key
-- @kube-hs.example/backup=yes@, keeps a "backup" ConfigMap (same data, name
-- suffixed @-backup@) in sync with it. This is the first example that
-- actually *writes* — via 'compileControllerWithWriter' (which wires
-- 'Kubernetes.Operator.Client.KubeWriter' in, so the reconciler can call
-- 'createResource'\\/'updateResource'), as opposed to the read-only
-- 'compileController'. Watch events give the controller a kick; the
-- reconcile loop turns them into idempotent create-or-update writes.
--
-- It also demonstrates owner references / garbage collection: the backup is
-- created with 'setControllerReference' pointing at its source, so when the
-- source ConfigMap is deleted, the real API server's garbage-collector
-- controller deletes the backup too — this operator never deletes anything
-- itself. (That's the same 'OwnerReference' machinery the comprehensive
-- example uses for its child objects, exercised here against a built-in
-- kind.) Writes use optimistic concurrency: 'updateResource' sends the
-- object's @resourceVersion@, so a concurrent change by someone else
-- surfaces as a 'Kubernetes.Client.KubeApiError' 409 Conflict — retried as
-- a transient error.
--
-- > cabal run configmap-backup
module Main (main) where

import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.:?), (.=))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Effectful (Eff)
import Kubernetes.Operator
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

data ConfigMap = ConfigMap
  { cmMeta :: ObjectMeta
  , cmData :: Map Text Text
  }
  deriving (Show)

instance FromJSON ConfigMap where
  parseJSON = withObject "ConfigMap" $ \o ->
    ConfigMap <$> o .: "metadata" <*> (fromMaybe Map.empty <$> o .:? "data")

instance ToJSON ConfigMap where
  toJSON cm =
    object
      [ "metadata" .= cmMeta cm
      , "data" .= cmData cm
      ]

instance Resource ConfigMap where
  resourceGVK _ = GVK "" "v1" "ConfigMap"
  resourceScope _ = Namespaced
  resourcePlural _ = "configmaps"
  resourceMeta = cmMeta
  resourceSetMeta m cm = cm {cmMeta = m}

backupKey :: Text
backupKey = "kube-hs-example-backup" -- valid ConfigMap data key (no '/'; '.' allowed, but a clean name reads better)

wantsBackup :: ConfigMap -> Bool
wantsBackup cm = Map.lookup backupKey (cmData cm) == Just "yes"

-- The backup object shares the namespace; we key it by the source's name so
-- a future example can attach an owner reference.
backupName :: ObjectKey -> Text
backupName (ObjectKey _ name) = name <> "-backup"

-- A backup mirrors the source's data except the operator's own tag key:
-- without stripping it, every backup would itself look like a source (it'd
-- carry @kube-hs-example-backup=yes@), and the operator would recursively
-- back up its own backups forever. This is the classic re-entrancy trap in
-- a controller that both watches a kind and creates that same kind.
backupData :: ConfigMap -> Map Text Text
backupData src = Map.delete backupKey (cmData src)

-- | Reads the source from the Cache (never trusts an event payload), then
-- ensures the backup exists and matches: create it if absent, update it if
-- its data has drifted. Returns 'Done' either way — reconciliation is
-- level-triggered, so a future watch event on the backup itself isn't
-- required to heal a missed write.
reconcileBackup :: (CtxRW ConfigMap es) => Request -> Eff es (Either ReconcileError ReconcileResult)
reconcileBackup (Request srcKey) = do
  mSrc <- cacheGet @ConfigMap srcKey
  case mSrc of
    Nothing -> do
      logInfo (renderKey srcKey <> " deleted, nothing to back up")
      pure (Right Done)
    Just src
      | not (wantsBackup src) -> do
          logInfo (renderKey srcKey <> " no backup tag, skipping")
          pure (Right Done)
      | otherwise -> do
          let dstKey = srcKey {okName = backupName srcKey}
          mDst <- getResource @ConfigMap dstKey
          case mDst of
            Nothing -> do
              -- Create the backup owned by its source, so deleting the
              -- source garbage-collects the backup server-side. Fails if the
              -- source has no UID yet (it must, since it came from the Cache
              -- post-LIST), which is a permanent error, not a retry.
              let rawBackup = src {cmData = backupData src, cmMeta = newMeta dstKey}
              case setControllerReference src rawBackup of
                Left err -> pure (Left (PermanentError err))
                Right backup -> do
                  _ <- createResource @ConfigMap backup
                  logInfo ("created backup " <> renderKey dstKey)
                  pure (Right Done)
            Just dst
              | cmData dst == backupData src -> do
                  logInfo (renderKey dstKey <> " already in sync")
                  pure (Right Done)
              | otherwise -> do
                  _ <- updateResource (dst {cmData = backupData src})
                  logInfo ("updated backup " <> renderKey dstKey)
                  pure (Right Done)
  where
    -- A fresh object we own: no resourceVersion yet (it'll be set by the
    -- API server on create), no finalizers/owner refs.
    newMeta (ObjectKey ns name) =
      ObjectMeta name ns Nothing Nothing Nothing [] []

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  kubeConfig <- loadKubeConfig
  let scope = case kcNamespace kubeConfig of
        AllNamespaces -> WatchAllNamespaces
        NS ns -> WatchNamespace ns

      spec :: ControllerSpecRW ConfigMap
      spec =
        ControllerSpecRW
          { crsName = "configmap-backup"
          , crsScope = scope
          , crsWorkers = 2
          , crsMaxRetries = 5
          , crsReconcile = reconcileBackup
          }

  metrics <- newMetricsRegistry
  controller <- compileControllerWithWriter kubeConfig defaultOperatorConfig metrics spec
  withMetricsServer 9090 metrics (runManager defaultManagerConfig [controller])
