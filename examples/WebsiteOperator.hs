{-# LANGUAGE OverloadedStrings #-}

-- | The comprehensive worked example: an operator for the generated
-- 'Website' CRD type (@examples\/generated\/Website.hs@, produced by
-- @crd-codegen@ from @examples\/crds\/website-crd.yaml@ — see the
-- top-level README for the exact command) that exercises every extension
-- point in one realistic reconciler:
--
--   * __finalizer__: adds one before doing anything else, and on deletion
--     runs a (simulated) cleanup action before removing it — see
--     "Kubernetes.Operator.Finalizer";
--   * __status subresource__: sets @status.readyReplicas@\/a @Ready@
--     condition from @spec.replicas@, guarded so it only writes when the
--     status actually changed;
--   * __metrics__: a counter bump on each real unit of work, via
--     "Kubernetes.Operator.Metrics".
--
-- For a shorter, read-only starting point see @ConfigMapLogger.hs@; for
-- leader election wrapped around a full 'Manager', see
-- @LeaderElectedConfigMapLogger.hs@.
module Main (main) where

import Data.Text (Text)
import qualified Data.Text as T
import Effectful (Eff)
import Kubernetes.Operator
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)
import Website

-- | Real CRDs should namespace their finalizer names to the group they
-- own, the same way Kubernetes itself does (e.g.
-- @kubernetes.io/pv-protection@) — it's what lets a cluster administrator
-- tell, just from `kubectl get -o yaml`, which controller a finalizer
-- belongs to.
websiteFinalizer :: Text
websiteFinalizer = "example.com/website-cleanup"

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
      logInfo ("website " <> renderKey key <> " gone")
      pure (Right Done)
    Just site
      | isBeingDeleted site -> finalize site
      | not (hasFinalizer websiteFinalizer site) -> addFinalizer site
      | otherwise -> syncStatus site
  where
    -- Deletion requested: run cleanup, then remove the finalizer — which
    -- is what actually lets Kubernetes finish deleting the object, since
    -- our finalizer is what's holding it back in the first place.
    finalize site = do
      cleaned <-
        finalizeAndRemove websiteFinalizer site $ do
          -- A real operator would release whatever `spec.domain` implied
          -- externally — a DNS record, a TLS certificate, a load balancer.
          logInfo ("website " <> renderKey key <> " releasing domain " <> websiteSpecDomain (websiteSpec site))
          incCounter "website_finalized_total"
          pure (Right ())
      pure (fmap (const Done) cleaned)

    -- Not being deleted, but hasn't got our finalizer yet: add it and
    -- persist. The write just changed resourceVersion, so the object
    -- we're holding is now stale — come back shortly rather than
    -- re-reading a Cache that hasn't caught up with our own write yet.
    addFinalizer site = do
      _ <- ensureFinalizer websiteFinalizer site
      pure (Right (RequeueAfter 0.5))

    -- Steady state: compute what status should be and write it if (and
    -- only if) it's actually different — see 'reconcileWebsite''s Haddock
    -- on why the unconditional version loops forever.
    syncStatus site = do
      let spec = websiteSpec site
          newStatus =
            WebsiteStatus
              { websiteStatusReadyReplicas = Just (websiteSpecReplicas spec)
              , websiteStatusConditions = Just [readyCondition]
              }
      if websiteStatus site == Just newStatus
        then pure (Right Done)
        else do
          _ <- updateStatus (site {websiteStatus = Just newStatus})
          incCounter "website_reconciled_total"
          logInfo
            ( "website "
                <> renderKey key
                <> " domain="
                <> websiteSpecDomain spec
                <> " replicas="
                <> tshow (websiteSpecReplicas spec)
            )
          pure (Right Done)

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
