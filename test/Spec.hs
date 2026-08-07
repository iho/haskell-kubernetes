{-# LANGUAGE OverloadedStrings #-}

-- | Integration tests, run against real (if fake) HTTP servers via
-- "Network.Wai.Handler.Warp" on ephemeral ports — no fixed port, no
-- external process, so this is safe to run concurrently and in CI.
--
-- Rather than scraping log output (the 'Log' effect's interpreter,
-- 'Kubernetes.Client.runLogIO', is the only one exposed — deliberately, see
-- "Kubernetes.Client"'s Haddock — so a test can't swap in a capturing one
-- without reaching into the library), both scenarios record what they saw
-- directly into a 'TVar' closed over from 'main'. That is arguably the
-- better test design anyway: assert on structured facts, not printed text.
module Main (main) where

import Control.Concurrent (threadDelay)
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.STM
import Control.Exception (SomeException, catch)
import Control.Monad (unless)
import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.=))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.ByteString.Builder as BB
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Effectful (Eff, liftIO, runEff)
import Kubernetes.Operator
import qualified Network.HTTP.Types as HTTP
import qualified Network.Wai as Wai
import qualified Network.Wai.Handler.Warp as Warp
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

-- --------------------------------------------------------------------------
-- Scenario 1: the read-only pipeline — generic 'KubeClient' -> 'Reflector'
-- -> 'Cache'\/'Workqueue' -> Controller worker pool -> 'Manager'.
-- --------------------------------------------------------------------------

newtype TestObj = TestObj {toMeta :: ObjectMeta}
  deriving (Show)

instance FromJSON TestObj where
  parseJSON = withObject "TestObj" $ \o -> TestObj <$> o .: "metadata"

instance Resource TestObj where
  resourceGVK _ = GVK "" "v1" "TestObj"
  resourceScope _ = Namespaced
  resourcePlural _ = "testobjs"
  resourceMeta = toMeta
  resourceSetMeta m obj = obj {toMeta = m}

-- | One initial LIST (obj-a, obj-b), then a watch stream with one ADDED,
-- one MODIFIED (of obj-a, at a new resourceVersion), and one DELETED (of
-- obj-b), then the stream ends (prompting the Reflector to resync,
-- exercised implicitly since the test only waits for the first pass
-- through).
jsonObj :: Text -> Text -> Aeson.Value
jsonObj name rv =
  object
    [ "metadata"
        .= object
          [ "name" .= name
          , "namespace" .= ("default" :: Text)
          , "resourceVersion" .= rv
          ]
    ]

watchEvent :: Text -> Aeson.Value -> Aeson.Value
watchEvent ty obj = object ["type" .= ty, "object" .= obj]

listBody :: Aeson.Value
listBody =
  object
    [ "metadata" .= object ["resourceVersion" .= ("100" :: Text)]
    , "items" .= [jsonObj "obj-a" "90", jsonObj "obj-b" "95"]
    ]

isWatchRequest :: Wai.Request -> Bool
isWatchRequest req = lookup "watch" (Wai.queryString req) == Just (Just "1")

sendJson :: (ToJSON a) => (BB.Builder -> IO ()) -> a -> IO ()
sendJson write v = write (BB.lazyByteString (Aeson.encode v) <> BB.byteString "\n")

fakeApiServer :: Wai.Application
fakeApiServer req respond
  | isWatchRequest req =
      respond $
        Wai.responseStream HTTP.status200 [("Content-Type", "application/json")] $ \write flush -> do
          sendJson write (watchEvent "ADDED" (jsonObj "obj-c" "101"))
          flush
          threadDelay 50000
          sendJson write (watchEvent "MODIFIED" (jsonObj "obj-a" "102"))
          flush
          threadDelay 50000
          sendJson write (watchEvent "DELETED" (jsonObj "obj-b" "103"))
          flush
  | otherwise =
      respond (Wai.responseLBS HTTP.status200 [("Content-Type", "application/json")] (Aeson.encode listBody))

data Seen
  = SeenPresent Text Text -- key, resourceVersion
  | SeenAbsent Text -- key
  deriving (Show, Eq)

recordingReconciler
  :: TVar [Seen]
  -> (forall es. (Ctx TestObj es) => Request -> Eff es (Either ReconcileError ReconcileResult))
recordingReconciler seenRef (Request key) = do
  mObj <- cacheGet @TestObj key
  let event = case mObj of
        Nothing -> SeenAbsent (renderKey key)
        Just obj -> SeenPresent (renderKey key) (fromMaybe "?" (omResourceVersion (toMeta obj)))
  liftIO (atomically (modifyTVar' seenRef (event :)))
  pure (Right Done)

pipelineScenario :: IO Bool
pipelineScenario = do
  seenRef <- newTVarIO []
  Warp.testWithApplication (pure fakeApiServer) $ \port -> do
    let cfg =
          KubeConfig
            { kcBaseUrl = "http://127.0.0.1:" <> T.pack (show port)
            , kcToken = Nothing
            , kcCaFile = Nothing
            , kcInsecureSkipTLSVerify = False
            , kcNamespace = NS "default"
            }
        spec :: ControllerSpec TestObj
        spec =
          ControllerSpec
            { csName = "test-obj-controller"
            , csScope = WatchNamespace "default"
            , csWorkers = 2
            , csMaxRetries = 3
            , csReconcile = recordingReconciler seenRef
            }
    controller <- compileController cfg defaultOperatorConfig spec
    runAsync <- Async.async (ccRun controller)
    threadDelay 800000 -- let the LIST and the full watch stream be processed
    ccShutdown controller
    Async.wait runAsync

  seen <- reverse <$> readTVarIO seenRef
  reportExpectations
    "pipeline"
    [ ("initial LIST: obj-a present at rv=90", SeenPresent "default/obj-a" "90" `elem` seen)
    , ("initial LIST: obj-b present at rv=95", SeenPresent "default/obj-b" "95" `elem` seen)
    , ("ADDED: obj-c observed at rv=101", SeenPresent "default/obj-c" "101" `elem` seen)
    , ("MODIFIED: obj-a's cached copy updated to rv=102", SeenPresent "default/obj-a" "102" `elem` seen)
    , ("DELETED: obj-b observed as absent from the cache", SeenAbsent "default/obj-b" `elem` seen)
    ]
    (show seen)

-- --------------------------------------------------------------------------
-- Scenario 2: 'KubeWriter' + "Kubernetes.Operator.Finalizer", exercised
-- directly (no Controller/Reflector/Cache/Workqueue needed — 'ensureFinalizer'
-- and 'finalizeAndRemove' are just 'Eff' actions) against a fake server that
-- records every PUT body it receives and echoes it back, the way a real API
-- server's response to a successful write looks to this client.
-- --------------------------------------------------------------------------

data FinObj = FinObj {foMeta :: ObjectMeta}
  deriving (Show)

instance FromJSON FinObj where
  parseJSON = withObject "FinObj" $ \o -> FinObj <$> o .: "metadata"

instance ToJSON FinObj where
  toJSON fo = object ["metadata" .= foMeta fo]

instance Resource FinObj where
  resourceGVK _ = GVK "" "v1" "FinObj"
  resourceScope _ = Namespaced
  resourcePlural _ = "finobjs"
  resourceMeta = foMeta
  resourceSetMeta m fo = fo {foMeta = m}

finalizerName :: Text
finalizerName = "test.example.com/cleanup"

writerFakeServer :: TVar [Aeson.Value] -> Wai.Application
writerFakeServer putBodiesRef req respond
  | Wai.requestMethod req == HTTP.methodPut = do
      body <- Wai.strictRequestBody req
      case Aeson.decode body of
        Nothing -> respond (Wai.responseLBS HTTP.status400 [] "bad JSON body")
        Just v -> do
          atomically (modifyTVar' putBodiesRef (++ [v]))
          respond (Wai.responseLBS HTTP.status200 [("Content-Type", "application/json")] body)
  | otherwise = respond (Wai.responseLBS HTTP.status404 [] "unused by this scenario")

finalizerScenario :: IO Bool
finalizerScenario = do
  putBodiesRef <- newTVarIO []
  cleanupRanRef <- newTVarIO False
  Warp.testWithApplication (pure (writerFakeServer putBodiesRef)) $ \port -> do
    let cfg =
          KubeConfig
            { kcBaseUrl = "http://127.0.0.1:" <> T.pack (show port)
            , kcToken = Nothing
            , kcCaFile = Nothing
            , kcInsecureSkipTLSVerify = False
            , kcNamespace = NS "default"
            }
        scope = WatchNamespace "default"
        objLive = FinObj (ObjectMeta "fin-a" (Just "default") (Just "10") Nothing Nothing [])
    mgr <- newManagerFor cfg
    runEff . runKubeWriterIO @FinObj mgr cfg scope $ do
      -- 1. Not being deleted, finalizer absent: ensureFinalizer should add
      -- it (a PUT) and report that it did.
      added <- ensureFinalizer finalizerName objLive
      liftIO (unless added (hPutStrLn stderr "FAILED: ensureFinalizer reported no change on a finalizer-less object"))

      -- 2. Simulate what the API server would show next: deletion
      -- requested, our finalizer (and only ours) still present.
      let objBeingDeleted = FinObj (ObjectMeta "fin-a" (Just "default") (Just "11") Nothing (Just "2024-01-01T00:00:00Z") [finalizerName])
      result <-
        finalizeAndRemove
          finalizerName
          objBeingDeleted
          (liftIO (atomically (writeTVar cleanupRanRef True)) >> pure (Right ()))
      liftIO $ case result of
        Left err -> hPutStrLn stderr ("FAILED: finalizeAndRemove reported an error: " <> show err)
        Right () -> pure ()

  cleanupRan <- readTVarIO cleanupRanRef
  bodies <- readTVarIO putBodiesRef
  let decoded = mapMaybe (Aeson.parseMaybe Aeson.parseJSON) bodies :: [FinObj]
      finalizerListsSeen = map (omFinalizers . foMeta) decoded
  reportExpectations
    "finalizer"
    [ ("exactly two writes observed (add, then remove)", length bodies == 2)
    , ("one write added the finalizer", [finalizerName] `elem` finalizerListsSeen)
    , ("one write removed it again", [] `elem` finalizerListsSeen)
    , ("the cleanup action actually ran before the finalizer was removed", cleanupRan)
    ]
    (show finalizerListsSeen)

-- --------------------------------------------------------------------------

reportExpectations :: String -> [(String, Bool)] -> String -> IO Bool
reportExpectations scenario expectations context = do
  let failures = [name | (name, ok) <- expectations, not ok]
  if null failures
    then do
      putStrLn ("OK [" <> scenario <> "]: all " <> show (length expectations) <> " expectations met")
      pure True
    else do
      hPutStrLn stderr ("FAILED [" <> scenario <> "]: " <> show failures)
      hPutStrLn stderr ("  context: " <> context)
      pure False

main :: IO ()
main = do
  r1 <- pipelineScenario
  r2 <-
    finalizerScenario `catch` \(e :: SomeException) -> do
      hPutStrLn stderr ("FAILED [finalizer]: scenario crashed: " <> show e)
      pure False
  if r1 && r2
    then putStrLn "ALL SCENARIOS PASSED"
    else exitFailure
