{-# LANGUAGE OverloadedStrings #-}

-- | Admission webhooks: the synchronous half of an operator, called by the
-- API server itself during @kubectl apply@ (before the object is even
-- persisted), as opposed to everything else in this library, which reacts
-- /after/ the fact via watch events.
--
-- Deliberately its own small, self-contained module with no dependency on
-- 'Kubernetes.Operator.Controller' or 'Kubernetes.Operator.Manager' — a
-- webhook server has nothing to do with the Reflector\/Cache\/Workqueue
-- machinery, it's a plain HTTP(S) server the API server calls directly, so
-- it gets its own minimal WAI plumbing instead.
--
-- Kubernetes requires admission webhooks to be served over TLS; this
-- module serves it, but does not generate or manage the certificate —
-- that's a deployment concern (in a real cluster, normally cert-manager's
-- job), not something the running process should be doing to itself. See
-- the accompanying example and README for how to generate one for local
-- testing.
module Kubernetes.Operator.Webhook
  ( -- * The AdmissionReview protocol
    AdmissionRequest (..)
  , PatchOp
  , jsonPatchAdd
  , jsonPatchReplace
  , jsonPatchRemove
  , AdmissionDecision (..)

    -- * Typed handlers
  , ValidatingHandler
  , MutatingHandler
  , validateTyped
  , mutateTyped

    -- * Serving
  , validatingWebhookApp
  , mutatingWebhookApp
  , runWebhookServerTLS
  ) where

import qualified Data.Aeson as Aeson
import Data.Aeson (FromJSON (..), ToJSON, Value, object, withObject, (.:), (.:?), (.=))
import qualified Data.Aeson.Types as Aeson
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Lazy as BL
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Network.HTTP.Types as HTTP
import qualified Network.Wai as Wai
import Network.Wai.Handler.WarpTLS (runTLS, tlsSettings)
import qualified Network.Wai.Handler.Warp as Warp

-- --------------------------------------------------------------------------
-- The AdmissionReview protocol (admission.k8s.io/v1)
-- --------------------------------------------------------------------------

-- | The parts of an @AdmissionReview@ request a handler actually needs.
-- Kept close to raw JSON ('Value' for the object itself) rather than
-- decoded into a concrete 'Kubernetes.Resource.Resource' type here —
-- that's what 'validateTyped'\/'mutateTyped' are for, keeping this level
-- generic over which kind a given webhook is registered for.
data AdmissionRequest = AdmissionRequest
  { admUID :: !Text
  , admOperation :: !Text
  -- ^ @"CREATE"@\/@"UPDATE"@\/@"DELETE"@\/@"CONNECT"@, passed through
  -- unparsed — handlers that care can match on it directly.
  , admNamespace :: !(Maybe Text)
  , admName :: !(Maybe Text)
  , admObject :: !(Maybe Value)
  -- ^ Absent on DELETE.
  , admOldObject :: !(Maybe Value)
  -- ^ Present on UPDATE\/DELETE.
  }
  deriving (Show)

instance FromJSON AdmissionRequest where
  parseJSON = withObject "AdmissionRequest" $ \o ->
    AdmissionRequest
      <$> o .: "uid"
      <*> o .: "operation"
      <*> o .:? "namespace"
      <*> o .:? "name"
      <*> o .:? "object"
      <*> o .:? "oldObject"

newtype AdmissionReviewIn = AdmissionReviewIn {reviewRequest :: AdmissionRequest}

instance FromJSON AdmissionReviewIn where
  parseJSON = withObject "AdmissionReview" $ \o -> AdmissionReviewIn <$> o .: "request"

-- | One RFC 6902 JSON Patch operation. Mutating webhooks build these
-- explicitly by JSON Pointer path rather than by diffing an old and new
-- Haskell value — for the handful of fields a webhook typically defaults
-- or normalizes, writing exactly the patch you mean is simpler and more
-- predictable than a generic structural diff.
data PatchOp = PatchOp
  { poOp :: !Text
  , poPath :: !Text
  , poValue :: !(Maybe Value)
  }

instance ToJSON PatchOp where
  toJSON p = object (["op" .= poOp p, "path" .= poPath p] ++ maybe [] (\v -> ["value" .= v]) (poValue p))

jsonPatchAdd :: Text -> Value -> PatchOp
jsonPatchAdd path v = PatchOp "add" path (Just v)

jsonPatchReplace :: Text -> Value -> PatchOp
jsonPatchReplace path v = PatchOp "replace" path (Just v)

jsonPatchRemove :: Text -> PatchOp
jsonPatchRemove path = PatchOp "remove" path Nothing

-- | What a webhook decided. 'AllowPatch' with an empty list is the normal
-- "nothing to change" outcome for a mutating webhook — equivalent to
-- 'Allow', spelled out separately only because a mutating handler always
-- returns a (possibly empty) patch list rather than choosing between two
-- constructors itself.
data AdmissionDecision
  = Allow
  | AllowPatch ![PatchOp]
  | Deny !Text

renderReview :: Text -> AdmissionDecision -> Value
renderReview uid decision =
  object
    [ "apiVersion" .= ("admission.k8s.io/v1" :: Text)
    , "kind" .= ("AdmissionReview" :: Text)
    , "response" .= responseBody
    ]
  where
    responseBody = case decision of
      Allow -> object ["uid" .= uid, "allowed" .= True]
      Deny msg -> object ["uid" .= uid, "allowed" .= False, "status" .= object ["message" .= msg]]
      AllowPatch [] -> object ["uid" .= uid, "allowed" .= True]
      AllowPatch ops ->
        object
          [ "uid" .= uid
          , "allowed" .= True
          , "patchType" .= ("JSONPatch" :: Text)
          , "patch" .= TE.decodeUtf8 (B64.encode (BL.toStrict (Aeson.encode ops)))
          ]

-- --------------------------------------------------------------------------
-- Typed handlers
-- --------------------------------------------------------------------------

-- | 'Left' denies the request with the given message; 'Right' allows it.
type ValidatingHandler a = a -> Either Text ()

-- | The patch to apply, if any (empty list: allow unchanged).
type MutatingHandler a = a -> [PatchOp]

-- | Lift a handler that only needs to look at the decoded object.
-- Decode failure denies with the parse error — the object genuinely
-- couldn't be understood, which is itself something worth surfacing to
-- whoever ran @kubectl apply@ rather than silently allowing or dropping.
validateTyped :: (FromJSON a) => ValidatingHandler a -> Value -> Either Text ()
validateTyped f v = case Aeson.parseEither parseJSON v of
  Left err -> Left (T.pack err)
  Right a -> f a

-- | As 'validateTyped', but a decode failure yields no patch (rather than
-- denying) — a mutating webhook doesn't get to unilaterally reject a
-- request that a validating webhook (if any) hasn't; it can only choose
-- not to change anything.
mutateTyped :: (FromJSON a) => MutatingHandler a -> Value -> [PatchOp]
mutateTyped f v = either (const []) f (Aeson.parseEither parseJSON v)

-- --------------------------------------------------------------------------
-- Serving
-- --------------------------------------------------------------------------

jsonResponse :: Value -> Wai.Response
jsonResponse = Wai.responseLBS HTTP.status200 [("Content-Type", "application/json")] . Aeson.encode

badRequest :: Wai.Response
badRequest = Wai.responseLBS HTTP.status400 [] "invalid AdmissionReview request"

readReview :: Wai.Request -> IO (Maybe AdmissionRequest)
readReview req = do
  body <- Wai.strictRequestBody req
  pure (reviewRequest <$> Aeson.decode body)

-- | A WAI 'Wai.Application' serving a validating webhook at whatever path
-- it's mounted on (see the example for combining this with
-- 'mutatingWebhookApp' under one server by dispatching on
-- 'Wai.pathInfo'). @DELETE@ operations (no 'admObject') are allowed
-- unconditionally — there is nothing left to validate.
validatingWebhookApp :: (Value -> Either Text ()) -> Wai.Application
validatingWebhookApp validate req respond = do
  mReq <- readReview req
  case mReq of
    Nothing -> respond badRequest
    Just areq ->
      let decision = case admObject areq of
            Nothing -> Allow
            Just obj -> either Deny (const Allow) (validate obj)
       in respond (jsonResponse (renderReview (admUID areq) decision))

-- | As 'validatingWebhookApp', for a mutating webhook.
mutatingWebhookApp :: (Value -> [PatchOp]) -> Wai.Application
mutatingWebhookApp mutate req respond = do
  mReq <- readReview req
  case mReq of
    Nothing -> respond badRequest
    Just areq ->
      let ops = maybe [] mutate (admObject areq)
       in respond (jsonResponse (renderReview (admUID areq) (AllowPatch ops)))

-- | Serve the given 'Wai.Application' over TLS on @port@ using a
-- certificate\/key PEM pair. Kubernetes refuses to call a webhook over
-- plain HTTP, so there is no non-TLS variant of this function.
runWebhookServerTLS :: Warp.Port -> FilePath -> FilePath -> Wai.Application -> IO ()
runWebhookServerTLS port certFile keyFile = runTLS (tlsSettings certFile keyFile) (Warp.setPort port Warp.defaultSettings)
