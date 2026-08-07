-- | A minimal Kubernetes client: list Pods, then watch them for changes.
--
-- Effect design
-- =============
--
-- There are two effects, kept deliberately small and orthogonal:
--
--   * 'Log'        — structured-ish timestamped logging.
--   * 'KubeClient'  — talking to the Kubernetes API (list + watch).
--
-- Both are declared as /dynamic/ effectful effects (GADTs dispatched via
-- 'Effectful.Dispatch.Dynamic.interpret'), which is the idiomatic effectful
-- way to describe an operation whose implementation you want to swap out
-- (real HTTP here; a pure/in-memory fake in tests) without touching any
-- code written against the effect.
--
-- 'KubeClient' is deliberately /first-order/: 'OpenWatch' hands back an
-- opaque 'WatchHandle' and the caller pulls events from it one at a time
-- with 'NextEvent'. This avoids the complexity of higher-order effects
-- (passing callbacks that themselves run in @Eff es@) while still letting
-- the calling code, 'program', be written as an ordinary direct-style loop.
--
-- The actual HTTP/JSON/streaming machinery lives in the IO-based
-- interpreter 'runKubeClientIO' and is completely invisible to 'program'.
module Kubernetes.Client
  ( -- * Configuration
    KubeConfig (..)
  , Namespace (..)
  , newManagerFor
  , loadKubeConfig

    -- * Domain types
  , Pod (..)
  , PodMeta (..)
  , PodList (..)
  , WatchEvent (..)
  , WatchEventType (..)
  , Status (..)
  , podOf
  , statusOf

    -- * Errors
  , KubeApiError (..)

    -- * Log effect
  , Log
  , LogLevel (..)
  , logInfo
  , logWarn
  , logErr
  , runLogIO

    -- * KubeClient effect
  , KubeClient
  , WatchHandle
  , listPods
  , openWatch
  , nextEvent
  , closeWatch
  , runKubeClientIO

    -- * Program logic (pure orchestration, effect-polymorphic)
  , program
  ) where

import Control.Exception (Exception, throwIO)
import Data.Aeson
  ( FromJSON (..)
  , Value
  , eitherDecodeStrict
  , withObject
  , withText
  , (.:)
  , (.:?)
  )
import Data.Aeson.Types (parseMaybe)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import Data.Char (isSpace)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Encoding.Error as TE
import Data.Time (defaultTimeLocale, formatTime, getCurrentTime)
import Effectful
import Effectful.Dispatch.Dynamic (interpret, send)
import Effectful.Exception (finally)
import Network.HTTP.Client
  ( BodyReader
  , Manager
  , Request (..)
  , Response
  , brConsume
  , brRead
  , httpLbs
  , parseRequest
  , responseBody
  , responseClose
  , responseOpen
  , responseStatus
  , responseTimeout
  , responseTimeoutNone
  , setQueryString
  )
import Network.HTTP.Types (hAuthorization, statusCode, statusIsSuccessful)
import qualified Network.HTTP.Types as HTTP
import Network.Connection (TLSSettings (TLSSettings, TLSSettingsSimple))
import Network.HTTP.Client (defaultManagerSettings, newManager)
import Network.HTTP.Client.TLS (mkManagerSettings, newTlsManager, newTlsManagerWith)
import Network.TLS (ClientParams (clientShared), Shared (sharedCAStore), defaultParamsClient)
import Data.X509.CertificateStore (readCertificateStore)
import qualified Data.Text.IO as TIO
import System.Directory (doesFileExist)
import System.Environment (lookupEnv)
import System.IO (hPutStrLn, stderr)

-- --------------------------------------------------------------------------
-- Configuration
-- --------------------------------------------------------------------------

-- | Which Pods to list/watch: a single namespace, or the cluster-scoped
-- "all namespaces" endpoint.
data Namespace = NS !Text | AllNamespaces
  deriving (Show, Eq)

data KubeConfig = KubeConfig
  { kcBaseUrl :: !Text
    -- ^ e.g. @https:\/\/10.0.0.1:6443@ or @http:\/\/127.0.0.1:8001@ (kubectl proxy)
  , kcToken :: !(Maybe Text)
    -- ^ Bearer token, if any.
  , kcCaFile :: !(Maybe FilePath)
    -- ^ PEM CA bundle used to verify the API server's certificate.
  , kcInsecureSkipTLSVerify :: !Bool
    -- ^ Skip TLS verification entirely. Only for throwaway local testing.
  , kcNamespace :: !Namespace
  }
  deriving (Show)

-- | Build an http-client 'Manager' matching the transport implied by
-- 'KubeConfig': a CA-verified TLS manager, an insecure TLS manager (opt-in,
-- for quick tests against a self-signed cluster), the system trust store, or
-- plain HTTP for @kubectl proxy@. Shared by the Pod demo below and by the
-- operator framework's 'Kubernetes.Operator.Controller.compileController'.
newManagerFor :: KubeConfig -> IO Manager
newManagerFor cfg
  | kcInsecureSkipTLSVerify cfg =
      newTlsManagerWith (mkManagerSettings (TLSSettingsSimple True False False) Nothing)
  | Just caFile <- kcCaFile cfg = do
      sniHost <- hostFromUrl (kcBaseUrl cfg)
      mkTlsManagerFromCA caFile sniHost
  | "https:" `T.isPrefixOf` kcBaseUrl cfg = newTlsManager
  | otherwise = newManager defaultManagerSettings

hostFromUrl :: Text -> IO String
hostFromUrl url = do
  req <- parseRequest (T.unpack url)
  pure (BC.unpack (host req))

mkTlsManagerFromCA :: FilePath -> String -> IO Manager
mkTlsManagerFromCA caFile sniHost = do
  mStore <- readCertificateStore caFile
  store <- maybe (ioError (userError ("could not read CA file: " <> caFile))) pure mStore
  let baseParams = defaultParamsClient sniHost ""
      params = baseParams {clientShared = (clientShared baseParams) {sharedCAStore = store}}
  newTlsManagerWith (mkManagerSettings (TLSSettings params) Nothing)

saTokenPath, saCaPath :: FilePath
saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
saCaPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

-- | Config discovery, in order of preference:
--
-- 1. Explicit @KUBE_API_SERVER@ env var (plus optional @KUBE_TOKEN@,
--    @KUBE_CA_FILE@, @KUBE_INSECURE_SKIP_TLS_VERIFY=1@) — for pointing at a
--    real cluster from a laptop.
-- 2. The standard in-cluster ServiceAccount files/env vars, when present.
-- 3. Otherwise assume a local @kubectl proxy --port=8001@ (plain HTTP, no
--    auth needed — the proxy handles that).
--
-- Shared by the Pod demo below and by any operator built on
-- "Kubernetes.Operator" — every executable in this package discovers its
-- cluster connection the same way.
loadKubeConfig :: IO KubeConfig
loadKubeConfig = do
  ns <- loadNamespace
  mExplicitUrl <- lookupEnv "KUBE_API_SERVER"
  case mExplicitUrl of
    Just url -> do
      token <- fmap T.pack <$> lookupEnv "KUBE_TOKEN"
      caFile <- lookupEnv "KUBE_CA_FILE"
      insecure <- (== Just "1") <$> lookupEnv "KUBE_INSECURE_SKIP_TLS_VERIFY"
      pure
        KubeConfig
          { kcBaseUrl = T.pack url
          , kcToken = token
          , kcCaFile = caFile
          , kcInsecureSkipTLSVerify = insecure
          , kcNamespace = ns
          }
    Nothing -> do
      inCluster <- doesFileExist saTokenPath
      if inCluster
        then do
          token <- T.strip <$> TIO.readFile saTokenPath
          svcHost <- getEnvOrDie "KUBERNETES_SERVICE_HOST"
          svcPort <- fromMaybe "443" <$> lookupEnv "KUBERNETES_SERVICE_PORT"
          caExists <- doesFileExist saCaPath
          pure
            KubeConfig
              { kcBaseUrl = T.pack ("https://" <> svcHost <> ":" <> svcPort)
              , kcToken = Just token
              , kcCaFile = if caExists then Just saCaPath else Nothing
              , kcInsecureSkipTLSVerify = False
              , kcNamespace = ns
              }
        else
          pure
            KubeConfig
              { kcBaseUrl = "http://127.0.0.1:8001"
              , kcToken = Nothing
              , kcCaFile = Nothing
              , kcInsecureSkipTLSVerify = False
              , kcNamespace = ns
              }
  where
    getEnvOrDie name =
      lookupEnv name >>= maybe (ioError (userError ("missing required env var " <> name))) pure

loadNamespace :: IO Namespace
loadNamespace = do
  allNs <- (== Just "1") <$> lookupEnv "KUBE_ALL_NAMESPACES"
  if allNs
    then pure AllNamespaces
    else NS . T.pack . fromMaybe "default" <$> lookupEnv "KUBE_NAMESPACE"

-- --------------------------------------------------------------------------
-- Domain types + JSON
-- --------------------------------------------------------------------------

data PodMeta = PodMeta
  { pmName :: !Text
  , pmNamespace :: !(Maybe Text)
  , pmResourceVersion :: !(Maybe Text)
  }
  deriving (Show)

instance FromJSON PodMeta where
  parseJSON = withObject "PodMeta" $ \o ->
    PodMeta
      <$> o .: "name"
      <*> o .:? "namespace"
      <*> o .:? "resourceVersion"

newtype Pod = Pod {podMeta :: PodMeta}
  deriving (Show)

instance FromJSON Pod where
  parseJSON = withObject "Pod" $ \o -> Pod <$> o .: "metadata"

data PodList = PodList
  { plResourceVersion :: !Text
  , plItems :: ![Pod]
  }
  deriving (Show)

instance FromJSON PodList where
  parseJSON = withObject "PodList" $ \o -> do
    meta <- o .: "metadata"
    rv <- meta .: "resourceVersion"
    items <- o .: "items"
    pure (PodList rv items)

data WatchEventType = Added | Modified | Deleted | Bookmark | ErrorEvt
  deriving (Show, Eq)

instance FromJSON WatchEventType where
  parseJSON = withText "WatchEventType" $ \case
    "ADDED" -> pure Added
    "MODIFIED" -> pure Modified
    "DELETED" -> pure Deleted
    "BOOKMARK" -> pure Bookmark
    "ERROR" -> pure ErrorEvt
    other -> fail ("unknown watch event type: " <> T.unpack other)

-- | A single line of the watch stream. 'weObject' is kept as raw 'Value'
-- because its shape depends on 'weType': usually a @Pod@, but a @Status@
-- object when 'weType' is 'ErrorEvt'. Use 'podOf' \/ 'statusOf' to decode it.
data WatchEvent = WatchEvent
  { weType :: !WatchEventType
  , weObject :: !Value
  }
  deriving (Show)

instance FromJSON WatchEvent where
  parseJSON = withObject "WatchEvent" $ \o ->
    WatchEvent <$> o .: "type" <*> o .: "object"

data Status = Status
  { stMessage :: !(Maybe Text)
  , stReason :: !(Maybe Text)
  , stCode :: !(Maybe Int)
  }
  deriving (Show)

instance FromJSON Status where
  parseJSON = withObject "Status" $ \o ->
    Status <$> o .:? "message" <*> o .:? "reason" <*> o .:? "code"

podOf :: WatchEvent -> Maybe Pod
podOf evt = parseMaybe parseJSON (weObject evt)

statusOf :: WatchEvent -> Maybe Status
statusOf evt = parseMaybe parseJSON (weObject evt)

-- | Thrown by the 'KubeClient' interpreter for any non-2xx API response.
-- 'main' special-cases status 410 (Gone / "resourceVersion too old").
data KubeApiError = KubeApiError
  { kaeStatus :: !Int
  , kaeBody :: !Text
  }

instance Show KubeApiError where
  show (KubeApiError code body) = "Kubernetes API error " <> show code <> ": " <> T.unpack body

instance Exception KubeApiError

-- --------------------------------------------------------------------------
-- Log effect
-- --------------------------------------------------------------------------

data LogLevel = Info | Warn | Err
  deriving (Show, Eq)

data Log :: Effect where
  LogMsg :: LogLevel -> Text -> Log m ()

type instance DispatchOf Log = Dynamic

logInfo, logWarn, logErr :: (Log :> es) => Text -> Eff es ()
logInfo = send . LogMsg Info
logWarn = send . LogMsg Warn
logErr = send . LogMsg Err

-- | Print log lines to stdout with an ISO-8601 timestamp.
runLogIO :: (IOE :> es) => Eff (Log : es) a -> Eff es a
runLogIO = interpret $ \_ -> \case
  LogMsg lvl msg -> liftIO $ do
    now <- getCurrentTime
    let ts = formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%3QZ" now
    putStrLn ("[" <> ts <> "] " <> show lvl <> "  " <> T.unpack msg)

-- --------------------------------------------------------------------------
-- KubeClient effect
-- --------------------------------------------------------------------------

-- | An open watch connection. Opaque to callers: pull events with
-- 'nextEvent', release the connection with 'closeWatch'.
data WatchHandle = WatchHandle
  { whResponse :: !(Response BodyReader)
  , whBuffer :: !(IORef BS.ByteString)
  }

data KubeClient :: Effect where
  ListPods :: KubeClient m PodList
  OpenWatch :: Text -> KubeClient m WatchHandle
  NextEvent :: WatchHandle -> KubeClient m (Maybe WatchEvent)
  CloseWatch :: WatchHandle -> KubeClient m ()

type instance DispatchOf KubeClient = Dynamic

listPods :: (KubeClient :> es) => Eff es PodList
listPods = send ListPods

-- | Open a watch starting just after the given @resourceVersion@ (normally
-- the one returned by the preceding LIST).
openWatch :: (KubeClient :> es) => Text -> Eff es WatchHandle
openWatch = send . OpenWatch

-- | Block for the next NDJSON event. 'Nothing' means the server closed the
-- stream (e.g. after the watch timeout elapsed).
nextEvent :: (KubeClient :> es) => WatchHandle -> Eff es (Maybe WatchEvent)
nextEvent = send . NextEvent

closeWatch :: (KubeClient :> es) => WatchHandle -> Eff es ()
closeWatch = send . CloseWatch

-- | Real HTTP interpreter for 'KubeClient'. This is the only place in the
-- program that knows about http-client, TLS, or NDJSON framing.
runKubeClientIO :: (IOE :> es) => Manager -> KubeConfig -> Eff (KubeClient : es) a -> Eff es a
runKubeClientIO mgr cfg = interpret $ \_ -> \case
  ListPods -> liftIO (doListPods mgr cfg)
  OpenWatch rv -> liftIO (doOpenWatch mgr cfg rv)
  NextEvent wh -> liftIO (readNextEvent wh)
  CloseWatch wh -> liftIO (responseClose (whResponse wh))

podsPath :: KubeConfig -> String
podsPath cfg = case kcNamespace cfg of
  AllNamespaces -> "/api/v1/pods"
  NS ns -> "/api/v1/namespaces/" <> T.unpack ns <> "/pods"

buildRequest :: KubeConfig -> String -> [(BS.ByteString, Maybe BS.ByteString)] -> IO Request
buildRequest cfg path query = do
  base <- parseRequest (T.unpack (kcBaseUrl cfg) <> path)
  let withAuth = case kcToken cfg of
        Nothing -> base
        Just tok ->
          base
            { requestHeaders = (hAuthorization, TE.encodeUtf8 ("Bearer " <> tok)) : requestHeaders base
            }
  pure (setQueryString query withAuth)

checkStatus :: HTTP.Status -> BL.ByteString -> IO ()
checkStatus st body
  | statusIsSuccessful st = pure ()
  | otherwise = throwIO (KubeApiError (statusCode st) (bodyAsText (BL.toStrict body)))
  where
    bodyAsText = TE.decodeUtf8With TE.lenientDecode

doListPods :: Manager -> KubeConfig -> IO PodList
doListPods mgr cfg = do
  req <- buildRequest cfg (podsPath cfg) []
  resp <- httpLbs req mgr
  checkStatus (responseStatus resp) (responseBody resp)
  case eitherDecodeStrict (BL.toStrict (responseBody resp)) of
    Left err -> throwIO (KubeApiError 0 ("failed to decode PodList: " <> T.pack err))
    Right podList -> pure podList

doOpenWatch :: Manager -> KubeConfig -> Text -> IO WatchHandle
doOpenWatch mgr cfg rv = do
  req0 <-
    buildRequest
      cfg
      (podsPath cfg)
      [ ("watch", Just "1")
      , ("resourceVersion", Just (TE.encodeUtf8 rv))
      , ("timeoutSeconds", Just "600")
      , ("allowWatchBookmarks", Just "true")
      ]
  let req = req0 {responseTimeout = responseTimeoutNone}
  resp <- responseOpen req mgr
  let st = responseStatus resp
  if statusIsSuccessful st
    then do
      ref <- newIORef BS.empty
      pure (WatchHandle resp ref)
    else do
      chunks <- brConsume (responseBody resp)
      responseClose resp
      throwIO (KubeApiError (statusCode st) (TE.decodeUtf8With TE.lenientDecode (BS.concat chunks)))

-- | Read one NDJSON line out of the watch body, buffering partial chunks.
-- Blank lines (occasionally used as keep-alives) are skipped; lines that
-- fail to parse are logged to stderr and skipped rather than aborting the
-- whole watch.
readNextEvent :: WatchHandle -> IO (Maybe WatchEvent)
readNextEvent wh = loop
  where
    loop = do
      buf <- readIORef (whBuffer wh)
      case BS.elemIndex 10 {- '\n' -} buf of
        Just i -> do
          let (line, rest) = BS.splitAt i buf
          writeIORef (whBuffer wh) (BS.drop 1 rest)
          case BC.dropWhile isSpace line of
            l | BS.null l -> loop
            l -> case eitherDecodeStrict l of
              Right evt -> pure (Just evt)
              Left err -> do
                hPutStrLn stderr ("watch: failed to parse event, skipping: " <> err)
                loop
        Nothing -> do
          chunk <- brRead (responseBody (whResponse wh))
          if BS.null chunk
            then pure Nothing -- server closed the stream
            else do
              writeIORef (whBuffer wh) (buf <> chunk)
              loop

-- --------------------------------------------------------------------------
-- Program logic — pure orchestration over the two effects above
-- --------------------------------------------------------------------------

-- | LIST pods, print them, then WATCH from the returned resourceVersion
-- until the server closes the stream, an ERROR event arrives (typically
-- "resourceVersion too old" / 410 Gone), or we're interrupted.
--
-- This function only ever talks to 'KubeClient' and 'Log' — it has no idea
-- whether it's running against a real cluster or a test double.
program :: (KubeClient :> es, Log :> es, IOE :> es) => Eff es ()
program = do
  logInfo "Listing pods..."
  podList <- listPods
  logInfo
    ( T.pack (show (length (plItems podList)))
        <> " pod(s) found (resourceVersion="
        <> plResourceVersion podList
        <> ")"
    )
  mapM_ (logInfo . describePod) (plItems podList)

  logInfo "Starting watch..."
  wh <- openWatch (plResourceVersion podList)
  watchLoop wh `finally` do
    logInfo "Closing watch connection"
    closeWatch wh

watchLoop :: (KubeClient :> es, Log :> es, IOE :> es) => WatchHandle -> Eff es ()
watchLoop wh = do
  mEvt <- nextEvent wh
  case mEvt of
    Nothing -> logInfo "Watch stream ended by server"
    Just evt
      | weType evt == ErrorEvt -> do
          logInfo (describeEvent evt)
          logInfo "Received ERROR event (commonly: resourceVersion too old / 410 Gone); stopping watch cleanly."
      | otherwise -> do
          logInfo (describeEvent evt)
          watchLoop wh

describePod :: Pod -> Text
describePod p =
  "  - "
    <> pmName (podMeta p)
    <> maybe "" (\ns -> " (ns=" <> ns <> ")") (pmNamespace (podMeta p))

eventTypeLabel :: WatchEventType -> Text
eventTypeLabel = \case
  Added -> "ADDED"
  Modified -> "MODIFIED"
  Deleted -> "DELETED"
  Bookmark -> "BOOKMARK"
  ErrorEvt -> "ERROR"

describeEvent :: WatchEvent -> Text
describeEvent evt = case weType evt of
  ErrorEvt ->
    "EVENT ERROR: " <> case statusOf evt of
      Just s -> fromMaybe "<no message>" (stMessage s) <> maybe "" ((" code=" <>) . T.pack . show) (stCode s)
      Nothing -> T.pack (show (weObject evt))
  t -> case podOf evt of
    Just pod ->
      eventTypeLabel t
        <> " pod="
        <> pmName (podMeta pod)
        <> " resourceVersion="
        <> fromMaybe "-" (pmResourceVersion (podMeta pod))
    Nothing -> eventTypeLabel t <> " <object did not parse as Pod>"
