{-# LANGUAGE OverloadedStrings #-}

-- | A validating + mutating admission webhook for the generated 'Website'
-- CRD type, served over TLS on one port at two paths. Unlike every other
-- example in this directory, this one is called synchronously by the API
-- server itself during @kubectl apply@ — a rejection here means the
-- object is never even persisted, not just never reconciled.
--
-- Validates: @spec.replicas@ must be at most 10.
-- Mutates: defaults @spec.tlsEnabled@ to @true@ when the field is absent.
--
-- Requires a TLS certificate whose SAN matches the Service this is
-- deployed behind — Kubernetes refuses to call a webhook endpoint whose
-- certificate doesn't validate. See the top-level README for how this was
-- generated and deployed for real-cluster testing.
--
-- > website-webhook server.crt server.key [port]
module Main (main) where

import Data.Aeson (Value (Bool))
import Data.Text (Text)
import qualified Data.Text as T
import Kubernetes.Operator.Webhook
import qualified Network.Wai as Wai
import qualified Network.HTTP.Types as HTTP
import System.Environment (getArgs)
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)
import Website

validateWebsite :: Website -> Either Text ()
validateWebsite site
  | websiteSpecReplicas spec > 10 = Left "spec.replicas must not exceed 10"
  | not (T.any (== '.') (websiteSpecDomain spec)) = Left "spec.domain must contain at least one '.'"
  | otherwise = Right ()
  where
    spec = websiteSpec site

mutateWebsite :: Website -> [PatchOp]
mutateWebsite site = case websiteSpecTlsEnabled (websiteSpec site) of
  Just _ -> []
  Nothing -> [jsonPatchAdd "/spec/tlsEnabled" (Bool True)]

app :: Wai.Application
app req respond = case Wai.pathInfo req of
  ["validate"] -> validatingWebhookApp (validateTyped validateWebsite) req respond
  ["mutate"] -> mutatingWebhookApp (mutateTyped mutateWebsite) req respond
  _ -> respond (Wai.responseLBS HTTP.status404 [] "not found (expected /validate or /mutate)")

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  args <- getArgs
  case args of
    [certFile, keyFile] -> serve certFile keyFile 8443
    [certFile, keyFile, portStr] -> serve certFile keyFile (read portStr)
    _ -> putStrLn "usage: website-webhook <cert.pem> <key.pem> [port]"
  where
    serve certFile keyFile port = do
      putStrLn ("serving on :" <> show port <> " (paths: /validate, /mutate)")
      runWebhookServerTLS port certFile keyFile app
