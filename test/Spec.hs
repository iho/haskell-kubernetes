{-# LANGUAGE OverloadedStrings #-}

-- | End-to-end integration test for the operator pipeline: generic
-- 'KubeClient' -> 'Reflector' -> 'Cache'\/'Workqueue' -> Controller worker
-- pool -> 'Manager', run against a real (if fake) HTTP server via
-- "Network.Wai.Handler.Warp" on an ephemeral port — no fixed port, no
-- external process, so this is safe to run concurrently and in CI.
--
-- Rather than scraping log output (the 'Log' effect's interpreter,
-- 'Kubernetes.Client.runLogIO', is the only one exposed — deliberately, see
-- "Kubernetes.Client"'s Haddock — so a test can't swap in a capturing one
-- without reaching into the library), the test reconciler records what it
-- saw directly into a 'TVar' closed over from 'main'. That is arguably the
-- better test design anyway: assert on structured facts, not printed text.
module Main (main) where

import Control.Concurrent (threadDelay)
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.STM
import Data.Aeson (FromJSON (..), ToJSON, object, withObject, (.:), (.=))
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Builder as BB
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Effectful (Eff, liftIO)
import Kubernetes.Operator
import qualified Network.HTTP.Types as HTTP
import qualified Network.Wai as Wai
import qualified Network.Wai.Handler.Warp as Warp
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

-- --------------------------------------------------------------------------
-- The resource under test
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

-- --------------------------------------------------------------------------
-- Fake API server: one initial LIST (obj-a, obj-b), then a watch stream
-- with one ADDED, one MODIFIED (of obj-a, at a new resourceVersion), and
-- one DELETED (of obj-b), then the stream ends (prompting the Reflector to
-- resync, exercised implicitly since the test only waits for the first
-- pass through).
-- --------------------------------------------------------------------------

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

-- --------------------------------------------------------------------------
-- Reconciler under test: records every observation instead of just logging
-- --------------------------------------------------------------------------

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

-- --------------------------------------------------------------------------

main :: IO ()
main = do
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
  let expectations :: [(String, Bool)]
      expectations =
        [ ("initial LIST: obj-a present at rv=90", SeenPresent "default/obj-a" "90" `elem` seen)
        , ("initial LIST: obj-b present at rv=95", SeenPresent "default/obj-b" "95" `elem` seen)
        , ("ADDED: obj-c observed at rv=101", SeenPresent "default/obj-c" "101" `elem` seen)
        , ("MODIFIED: obj-a's cached copy updated to rv=102", SeenPresent "default/obj-a" "102" `elem` seen)
        , ("DELETED: obj-b observed as absent from the cache", SeenAbsent "default/obj-b" `elem` seen)
        ]
      failures = [name | (name, ok) <- expectations, not ok]

  if null failures
    then putStrLn ("OK: all " <> show (length expectations) <> " expectations met (" <> show (length seen) <> " reconcile events observed)")
    else do
      hPutStrLn stderr ("FAILED: " <> show failures)
      hPutStrLn stderr ("Observed events: " <> show seen)
      exitFailure
