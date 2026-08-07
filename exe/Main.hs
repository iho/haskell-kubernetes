-- | Wiring: figure out how to talk to a cluster (in-cluster ServiceAccount,
-- an explicit token + CA, or a plain @kubectl proxy@), build an http-client
-- 'Manager' accordingly, then hand off to 'Kubernetes.Client.program'.
--
-- Nothing in here is effectful-specific: this module is the IO shell around
-- the effect stack, kept separate from the effect logic in
-- "Kubernetes.Client" on purpose.
module Main (main) where

import Control.Exception (AsyncException (UserInterrupt), catch, throwIO, try)
import qualified Data.ByteString.Char8 as BC
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Effectful (runEff)
import Kubernetes.Client
import Network.Connection (TLSSettings (TLSSettings, TLSSettingsSimple))
import Network.HTTP.Client
  ( Manager
  , defaultManagerSettings
  , host
  , newManager
  , parseRequest
  )
import Network.HTTP.Client.TLS (mkManagerSettings, newTlsManager, newTlsManagerWith)
import Network.TLS (ClientParams (clientShared), Shared (sharedCAStore), defaultParamsClient)
import Data.X509.CertificateStore (readCertificateStore)
import System.Directory (doesFileExist)
import System.Environment (lookupEnv)
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

saTokenPath, saCaPath :: FilePath
saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
saCaPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

-- | Config discovery, in order of preference:
--
-- 1. Explicit @KUBE_API_SERVER@ env var (plus optional @KUBE_TOKEN@,
--    @KUBE_CA_FILE@, @KUBE_INSECURE_SKIP_TLS_VERIFY=1@) — for pointing at a
--    real cluster from a laptop.
-- 2. The standard in-cluster ServiceAccount files/env vars, when present.
-- 3. Otherwise assume a local @kubectl proxy --port=8001@ (plain HTTP,
--    no auth needed — the proxy handles that).
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

-- | Build an http-client 'Manager' matching the transport implied by
-- 'KubeConfig': a CA-verified TLS manager, an insecure TLS manager (opt-in,
-- for quick tests against a self-signed cluster), the system trust store,
-- or plain HTTP for @kubectl proxy@.
newManagerFor :: KubeConfig -> IO Manager
newManagerFor cfg
  | kcInsecureSkipTLSVerify cfg =
      newTlsManagerWith (mkManagerSettings (TLSSettingsSimple True False False) Nothing)
  | Just caFile <- kcCaFile cfg = do
      sniHost <- hostFromUrl (kcBaseUrl cfg)
      mkTlsManagerFromCA caFile sniHost
  | "https:" `T.isPrefixOf` kcBaseUrl cfg = newTlsManager
  | otherwise = newManager defaultManagerSettings

hostFromUrl :: T.Text -> IO String
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

main :: IO ()
main = run `catch` handleInterrupt
  where
    run = do
      cfg <- loadKubeConfig
      mgr <- newManagerFor cfg
      result <- try (runEff . runLogIO $ runKubeClientIO mgr cfg program)
      case result of
        Right () -> pure ()
        Left err -> handleFailure err

    handleFailure :: KubeApiError -> IO ()
    handleFailure err@(KubeApiError 410 _) =
      putStrLn ("Watch expired (410 Gone): " <> show err <> ". Exiting cleanly (re-list on expiry is left as a future improvement).")
    handleFailure err = do
      hPutStrLn stderr ("Fatal: " <> show err)
      exitFailure

    handleInterrupt :: AsyncException -> IO ()
    handleInterrupt UserInterrupt = putStrLn "Interrupted by user, shutting down."
    handleInterrupt e = throwIO e
