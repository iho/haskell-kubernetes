{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

-- | A /cross-kind/ operator, the pattern the README lists as a known
-- limitation: a reconciler for one kind (the custom 'Service' CRD) that
-- creates and owns a *different* kind (a built-in 'Deployment'). The
-- framework's 'compileControllerWithWriter' wires 'KubeWriter' in for the
-- kind being reconciled (the CR), but not for the child kind — so the
-- reconciler opens a second interpreter stack for 'Deployment' itself, via
-- 'newManagerFor' + 'runKubeClientIO'\\/'runKubeWriterIO'. That's the exact
-- escape hatch "Kubernetes.Operator.OwnerReference"'s real-cluster
-- validation uses, now shown end to end.
--
-- For every @Service@ CR, this ensures a Deployment exists named after it
-- (running the requested image/replicas), owns it via 'setControllerReference'
-- so deleting the CR garbage-collects the Deployment, and patches the CR's
-- status to report the Deployment's ready replica count. Read-side writes
-- (CR status) go through the framework's 'updateStatus'; child writes go
-- through the hand-rolled 'Deployment' stack.
--
-- > cabal run service-deployer
module Main (main) where

import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.:?), (.=))
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Effectful (Eff, liftIO, runEff)
import Kubernetes.Operator
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

-- --------------------------------------------------------------------------
-- The CRD kind (hand-written 'Resource' instance; nothing here needed
-- @crd-codegen@).
-- --------------------------------------------------------------------------

data ServiceSpec = ServiceSpec
  { serviceSpecImage :: Text
  , serviceSpecReplicas :: Int
  }

instance FromJSON ServiceSpec where
  parseJSON = withObject "ServiceSpec" $ \o ->
    ServiceSpec <$> o .: "image" <*> o .: "replicas"

instance ToJSON ServiceSpec where
  toJSON s =
    object
      [ "image" .= serviceSpecImage s
      , "replicas" .= serviceSpecReplicas s
      ]

data ServiceStatus = ServiceStatus
  { serviceStatusReadyReplicas :: Int
  }
  deriving (Show, Eq)

instance FromJSON ServiceStatus where
  parseJSON = withObject "ServiceStatus" $ \o ->
    ServiceStatus <$> (fromMaybe 0 <$> o .:? "readyReplicas")

instance ToJSON ServiceStatus where
  toJSON st = object ["readyReplicas" .= serviceStatusReadyReplicas st]

data Service = Service
  { serviceMeta :: ObjectMeta
  , serviceSpec :: ServiceSpec
  , serviceStatus :: Maybe ServiceStatus
  }

instance FromJSON Service where
  parseJSON v = do
    (m, s, st) <- envelopeParseJSON v
    pure (Service m s st)

instance ToJSON Service where
  toJSON x =
    envelopeToJSON
      (GVK "example.com" "v1" "Service")
      (serviceMeta x)
      (serviceSpec x)
      (serviceStatus x)

-- | The whole 'Resource' instance in one line (group "example.com"/version
-- "v1"/kind "Service"/plural "services", namespaced, metadata field
-- @serviceMeta@).
deriveResource ''Service "example.com" "v1" "Service" "services" True 'serviceMeta

-- --------------------------------------------------------------------------
-- The child kind: a Deployment. We model only the handful of fields a real
-- Deployment's JSON needs for create/update + status read.
-- --------------------------------------------------------------------------

data Deployment = Deployment
  { depMeta :: ObjectMeta
  , depImage :: Text
  , depReplicas :: Int
  , depReadyReplicas :: Maybe Int
  }

instance FromJSON Deployment where
  parseJSON = withObject "Deployment" $ \o -> do
    meta <- o .: "metadata"
    spec <- o .: "spec"
    containers <- spec .: "template" >>= (.: "spec") >>= (.: "containers")
    image <- case containers of
      [] -> pure "" -- degenerate; shouldn't happen for a Deployment we created
      (c : _) -> c .: "image"
    replicas <- fromMaybe 1 <$> spec .:? "replicas"
    status <- o .:? "status"
    ready <- maybe (pure Nothing) (.:? "readyReplicas") status
    pure (Deployment meta image replicas ready)

instance ToJSON Deployment where
  toJSON d =
    let appLabel = omName (depMeta d) -- the "app" selector/label value = the Deployment's own name
     in object
          [ "apiVersion" .= ("apps/v1" :: Text)
          , "kind" .= ("Deployment" :: Text)
          , "metadata" .= depMeta d
          , "spec"
              .= object
                [ "replicas" .= depReplicas d
                , "selector"
                    .= object ["matchLabels" .= object ["app" .= appLabel]]
                , "template"
                    .= object
                      [ "metadata" .= object ["labels" .= object ["app" .= appLabel]]
                      , "spec"
                          .= object
                            [ "containers"
                                .= [ object
                                       [ "name" .= ("app" :: Text)
                                       , "image" .= depImage d
                                       ]
                                   ]
                            ]
                      ]
                ]
          ]

-- | The child kind's 'Resource' instance, also generated in one line
-- (group "apps"/version "v1"/kind "Deployment"/plural "deployments",
-- namespaced, metadata field @depMeta@).
deriveResource ''Deployment "apps" "v1" "Deployment" "deployments" True 'depMeta

-- | The child's name derives from the CR's name.
depName :: Service -> Text
depName svc = omName (serviceMeta svc) <> "-deploy"

-- | The child's object key: same namespace as the CR, child-derived name.
depKey :: Service -> ObjectKey
depKey svc = ObjectKey (omNamespace (serviceMeta svc)) (depName svc)

-- --------------------------------------------------------------------------
-- Cross-kind write capability: a fully-interpreted stack for the child kind,
-- built by hand (the documented escape hatch — the framework only wires
-- 'KubeWriter' for the reconciled kind).
-- --------------------------------------------------------------------------

data ChildWrites = ChildWrites
  { cwGet :: ObjectKey -> IO (Maybe Deployment)
  , cwUpdate :: Deployment -> IO Deployment
  , cwCreate :: Deployment -> IO Deployment
  }

buildChildWrites :: KubeConfig -> IO ChildWrites
buildChildWrites kubeConfig = do
  mgr <- newManagerFor kubeConfig
  let scope = case kcNamespace kubeConfig of
        AllNamespaces -> WatchAllNamespaces
        NS ns -> WatchNamespace ns
  pure
    ChildWrites
      { cwGet = \key -> runEff . runKubeClientIO @Deployment mgr kubeConfig scope $ getResource key
      , cwUpdate = \d -> runEff . runKubeWriterIO @Deployment mgr kubeConfig scope $ updateResource d
      , cwCreate = \d -> runEff . runKubeWriterIO @Deployment mgr kubeConfig scope $ createResource d
      }

-- | Reconciler: ensure the child Deployment exists and matches the CR, own
-- it, and report its readiness back into the CR's status.
reconcileService :: ChildWrites -> (CtxRW Service es) => Request -> Eff es (Either ReconcileError ReconcileResult)
reconcileService child (Request key) = do
  mSvc <- cacheGet @Service key
  case mSvc of
    Nothing -> do
      logInfo (renderKey key <> " deleted")
      done
    Just svc -> do
      mDep <- liftIO (cwGet child (depKey svc))
      case mDep of
        Nothing -> do
          let raw =
                Deployment
                  (ObjectMeta (depName svc) (omNamespace (serviceMeta svc)) Nothing Nothing Nothing [] [])
                  (serviceSpecImage (serviceSpec svc))
                  (serviceSpecReplicas (serviceSpec svc))
                  Nothing
          case setControllerReference svc raw of
            Left err -> permanentError err
            Right dep -> do
              _ <- liftIO (cwCreate child dep)
              logInfo ("created deployment " <> depName svc)
              requeueAfter 1
        Just dep
          | depImage dep /= serviceSpecImage (serviceSpec svc)
              || depReplicas dep /= serviceSpecReplicas (serviceSpec svc) -> do
              let dep' = dep {depImage = serviceSpecImage (serviceSpec svc), depReplicas = serviceSpecReplicas (serviceSpec svc)}
              _ <- liftIO (cwUpdate child dep')
              logInfo ("updated deployment " <> depName svc)
              requeueAfter 1
          | otherwise -> do
              -- In sync spec-wise. Report readiness into the CR's status.
              -- We only watch Service CRs, NOT the child Deployment, so the
              -- Deployment's readiness can change with no watch event to
              -- re-trigger us. Requeue on a short timer while converging,
              -- and on a longer poll interval once in sync, so the operator
              -- keeps the child under observation despite not watching it.
              let ready = fromMaybe 0 (depReadyReplicas dep)
                  desiredStatus = ServiceStatus ready
              if serviceStatus svc == Just desiredStatus
                then requeueAfter 10 -- in sync: periodic re-poll
                else do
                  _ <- updateStatus (svc {serviceStatus = Just desiredStatus})
                  logInfo (renderKey key <> " readyReplicas=" <> T.pack (show ready))
                  requeueAfter 2

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  kubeConfig <- loadKubeConfig
  child <- buildChildWrites kubeConfig
  let scope = case kcNamespace kubeConfig of
        AllNamespaces -> WatchAllNamespaces
        NS ns -> WatchNamespace ns

      spec :: ControllerSpecRW Service
      spec =
        ControllerSpecRW
          { crsName = "service-deployer"
          , crsScope = scope
          , crsWorkers = 2
          , crsMaxRetries = 5
          , crsReconcile = reconcileService child
          }

  metrics <- newMetricsRegistry
  controller <- compileControllerWithWriter kubeConfig defaultOperatorConfig metrics spec
  withMetricsServer 9090 metrics (runManager defaultManagerConfig [controller])
