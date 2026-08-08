{-# LANGUAGE OverloadedStrings #-}

-- | An operator for the generated 'Website' CRD type
-- (@examples\/generated\/Website.hs@, produced by @crd-codegen@ from
-- @examples\/crds\/website-crd.yaml@ — see the top-level README for the
-- exact command). Sets @status.readyReplicas@ from @spec.replicas@ and a
-- @Ready@ condition, demonstrating a codegen'd type going through the
-- write path (status subresource) via 'ControllerSpecRW'.
module Main (main) where

import Data.Text (Text)
import qualified Data.Text as T
import Effectful (Eff)
import Kubernetes.Operator
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)
import Website

readyCondition :: WebsiteStatusConditionsItem
readyCondition =
  WebsiteStatusConditionsItem
    { websiteStatusConditionsItemType = "Ready"
    , websiteStatusConditionsItemStatus = "True"
    }

reconcileWebsite :: (CtxRW Website es) => Request -> Eff es (Either ReconcileError ReconcileResult)
reconcileWebsite (Request key) = do
  mSite <- cacheGet @Website key
  case mSite of
    Nothing -> do
      logInfo ("website " <> renderKey key <> " deleted")
      pure (Right Done)
    Just site -> do
      let spec = websiteSpec site
          newStatus =
            WebsiteStatus
              { websiteStatusReadyReplicas = Just (websiteSpecReplicas spec)
              , websiteStatusConditions = Just [readyCondition]
              }
      -- Only write if something actually changed: `updateStatus` bumps
      -- resourceVersion, which the Reflector sees as a MODIFIED event and
      -- re-enqueues this key — writing unconditionally on every reconcile
      -- would loop forever even though nothing is really changing.
      if websiteStatus site == Just newStatus
        then pure (Right Done)
        else do
          _ <- updateStatus (site {websiteStatus = Just newStatus})
          logInfo
            ( "website "
                <> renderKey key
                <> " domain="
                <> websiteSpecDomain spec
                <> " replicas="
                <> tshow (websiteSpecReplicas spec)
            )
          pure (Right Done)
  where
    tshow :: (Show a) => a -> Text
    tshow = T.pack . show

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  kubeConfig <- loadKubeConfig
  let scope = case kcNamespace kubeConfig of
        AllNamespaces -> WatchAllNamespaces
        NS ns -> WatchNamespace ns

      spec :: ControllerSpecRW Website
      spec =
        ControllerSpecRW
          { crsName = "website-operator"
          , crsScope = scope
          , crsWorkers = 2
          , crsMaxRetries = 5
          , crsReconcile = reconcileWebsite
          }

  controller <- compileControllerWithWriter kubeConfig defaultOperatorConfig spec
  runManager defaultManagerConfig [controller]
