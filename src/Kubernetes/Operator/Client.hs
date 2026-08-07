{-# LANGUAGE AllowAmbiguousTypes #-}

-- | The generalized 'KubeClient' effect (roadmap step 1, now implemented):
-- identical operations to the original, verified, Pod-only
-- @Kubernetes.Client.KubeClient@ — @ListResources@\/@OpenWatch@\/
-- @NextEvent@\/@CloseWatch@ — parametrized by resource type instead of
-- hardcoded to Pod. Two different kinds in play at once are simply two
-- different effects in the row, e.g.
-- @(KubeClient Pod :> es, KubeClient ConfigMap :> es)@ — no sum type, no
-- existential, nothing dynamic about *which* resource, only about *how*
-- each operation is interpreted.
--
-- The interpreter below is a mechanical generalization of
-- @Kubernetes.Client.runKubeClientIO@: identical NDJSON line-buffering and
-- @responseOpen@\/@brRead@ streaming code, now driven by 'resourcePlural' \/
-- 'resourceScope' \/ 'resourceGVK' instead of a hardcoded @\/pods@ path, and
-- decoding with the caller-supplied 'FromJSON' instance instead of the
-- hardcoded @Pod@\/@PodList@ types. 'Kubernetes.Client' itself is left
-- untouched (it's the already-verified demo); this module is where the
-- generalized version lives, reusing 'Kubernetes.Client.KubeApiError' and
-- 'Kubernetes.Client.newManagerFor' rather than duplicating them.
module Kubernetes.Operator.Client
  ( KubeClient
  , ListResult (..)
  , WatchHandle
  , WatchEvent (..)
  , WatchEventType (..)
  , listResources
  , openWatch
  , nextEvent
  , closeWatch
  , runKubeClientIO
  ) where

import Control.Exception (throwIO)
import Data.Aeson
  ( FromJSON (..)
  , Value
  , eitherDecodeStrict
  , withObject
  , withText
  , (.:)
  )
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import Data.Char (isSpace)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Encoding.Error as TE
import Effectful
import Effectful.Dispatch.Dynamic (interpret, send)
import Kubernetes.Client (KubeApiError (..), KubeConfig (..))
import Kubernetes.Resource (GVK (..), Resource (..), Scope (..), WatchScope (..))
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
import System.IO (hPutStrLn, stderr)

data KubeClient a :: Effect where
  ListResources :: KubeClient a m (ListResult a)
  OpenWatch :: Text -> KubeClient a m (WatchHandle a)
  NextEvent :: WatchHandle a -> KubeClient a m (Maybe (WatchEvent a))
  CloseWatch :: WatchHandle a -> KubeClient a m ()

type instance DispatchOf (KubeClient a) = Dynamic

data ListResult a = ListResult
  { lrResourceVersion :: !Text
  , lrItems :: ![a]
  }
  deriving (Show)

instance (FromJSON a) => FromJSON (ListResult a) where
  parseJSON = withObject "ListResult" $ \o -> do
    meta <- o .: "metadata"
    rv <- meta .: "resourceVersion"
    items <- o .: "items"
    pure (ListResult rv items)

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

-- | 'weObject' is kept as raw 'Value' (decode on demand, e.g. via
-- 'Data.Aeson.Types.parseMaybe' 'parseJSON') because its shape depends on
-- 'weType': normally an @a@, but a @Status@ object
-- (see 'Kubernetes.Client.Status') when 'weType' is 'ErrorEvt'.
data WatchEvent a = WatchEvent
  { weType :: !WatchEventType
  , weObject :: !Value
  }
  deriving (Show)

instance FromJSON (WatchEvent a) where
  parseJSON = withObject "WatchEvent" $ \o ->
    WatchEvent <$> o .: "type" <*> o .: "object"

-- | An open watch connection. Opaque to callers: pull events with
-- 'nextEvent', release with 'closeWatch'.
data WatchHandle a = WatchHandle
  { whResponse :: !(Response BodyReader)
  , whBuffer :: !(IORef BS.ByteString)
  }

listResources :: (Resource a, KubeClient a :> es) => Eff es (ListResult a)
listResources = send ListResources

-- | Open a watch starting just after the given @resourceVersion@ (normally
-- the one returned by the preceding LIST).
openWatch :: (Resource a, KubeClient a :> es) => Text -> Eff es (WatchHandle a)
openWatch = send . OpenWatch

-- | Block for the next NDJSON event. 'Nothing' means the server closed the
-- stream (e.g. after the watch timeout elapsed).
nextEvent :: (Resource a, KubeClient a :> es) => WatchHandle a -> Eff es (Maybe (WatchEvent a))
nextEvent = send . NextEvent

closeWatch :: (Resource a, KubeClient a :> es) => WatchHandle a -> Eff es ()
closeWatch = send . CloseWatch

-- | Real HTTP interpreter. 'WatchScope' picks which namespace(s) this
-- particular controller watches; see its Haddock for why that's not part
-- of 'KubeConfig'.
runKubeClientIO
  :: forall a es r
   . (Resource a, FromJSON a, IOE :> es)
  => Manager
  -> KubeConfig
  -> WatchScope
  -> Eff (KubeClient a : es) r
  -> Eff es r
runKubeClientIO mgr cfg scope = interpret $ \_ -> \case
  ListResources -> liftIO (doListResources @a mgr cfg scope)
  OpenWatch rv -> liftIO (doOpenWatch @a mgr cfg scope rv)
  NextEvent wh -> liftIO (readNextEvent wh)
  CloseWatch wh -> liftIO (responseClose (whResponse wh))

resourcePath :: forall a. (Resource a) => WatchScope -> String
resourcePath scope =
  let GVK grp ver _kind = resourceGVK (Nothing :: Maybe a)
      plural = T.unpack (resourcePlural (Nothing :: Maybe a))
      apiRoot
        | T.null grp = "/api/" <> T.unpack ver
        | otherwise = "/apis/" <> T.unpack grp <> "/" <> T.unpack ver
   in case resourceScope (Nothing :: Maybe a) of
        ClusterScoped -> apiRoot <> "/" <> plural
        Namespaced -> case scope of
          WatchAllNamespaces -> apiRoot <> "/" <> plural
          WatchNamespace ns -> apiRoot <> "/namespaces/" <> T.unpack ns <> "/" <> plural

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
  | otherwise = throwIO (KubeApiError (statusCode st) (TE.decodeUtf8With TE.lenientDecode (BL.toStrict body)))

doListResources :: forall a. (Resource a, FromJSON a) => Manager -> KubeConfig -> WatchScope -> IO (ListResult a)
doListResources mgr cfg scope = do
  req <- buildRequest cfg (resourcePath @a scope) []
  resp <- httpLbs req mgr
  checkStatus (responseStatus resp) (responseBody resp)
  case eitherDecodeStrict (BL.toStrict (responseBody resp)) of
    Left err -> throwIO (KubeApiError 0 ("failed to decode list: " <> T.pack err))
    Right result -> pure result

doOpenWatch :: forall a. (Resource a) => Manager -> KubeConfig -> WatchScope -> Text -> IO (WatchHandle a)
doOpenWatch mgr cfg scope rv = do
  req0 <-
    buildRequest
      cfg
      (resourcePath @a scope)
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

readNextEvent :: WatchHandle a -> IO (Maybe (WatchEvent a))
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
            then pure Nothing
            else do
              writeIORef (whBuffer wh) (buf <> chunk)
              loop
