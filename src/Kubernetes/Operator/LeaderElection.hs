{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | Leader election via a @coordination.k8s.io/v1@ @Lease@ — the same
-- primitive client-go's @leaderelection@ package (and hence virtually
-- every real-world Go operator) is built on, so a Haskell operator using
-- this can safely run alongside, or be migrated from, one written in Go
-- against the same Lease.
--
-- Deliberately plain 'IO', matching "Kubernetes.Operator.Manager": the
-- thing being guarded (typically 'Kubernetes.Operator.Manager.runManager'
-- with its compiled controllers) is already fully-interpreted 'IO' by the
-- time it gets here, so there's no 'Eff' row to preserve. Internally this
-- module runs its own tiny, self-contained 'KubeClient'\/'KubeWriter'
-- interpreter pair for 'Lease' — it doesn't need or use the
-- Reflector\/Cache\/Workqueue machinery at all, since leader election is
-- just "GET, maybe PUT or POST, repeat," not something that benefits from
-- being driven by a watch.
module Kubernetes.Operator.LeaderElection
  ( Lease (..)
  , LeaseSpec (..)
  , LeaderElectionConfig (..)
  , defaultLeaderElectionConfig
  , runWithLeaderElection
  ) where

import Control.Concurrent (threadDelay)
import qualified Control.Concurrent.Async as Async
import Control.Exception (SomeException, try)
import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.:?), (.=))
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time
  ( NominalDiffTime
  , UTCTime
  , defaultTimeLocale
  , diffUTCTime
  , formatTime
  , getCurrentTime
  , parseTimeM
  )
import Effectful (Eff, IOE, runEff, (:>))
import Kubernetes.Client (KubeConfig, newManagerFor)
import Kubernetes.Operator.Client
  ( KubeClient
  , KubeWriter
  , createResource
  , getResource
  , runKubeClientIO
  , runKubeWriterIO
  , updateResource
  )
import Kubernetes.Resource
  ( GVK (..)
  , ObjectKey (..)
  , ObjectMeta (..)
  , Resource (..)
  , Scope (..)
  , WatchScope (..)
  , apiVersionOf
  )
import System.IO (hPutStrLn, stderr)

-- --------------------------------------------------------------------------
-- The Lease resource
-- --------------------------------------------------------------------------

data LeaseSpec = LeaseSpec
  { lsHolderIdentity :: !(Maybe Text)
  , lsLeaseDurationSeconds :: !(Maybe Int)
  , lsRenewTime :: !(Maybe Text)
  -- ^ RFC 3339 with microsecond precision, matching @metav1.MicroTime@ —
  -- kept as raw 'Text' (parsed\/formatted only where compared) rather than
  -- a real time type in the record, since round-tripping through
  -- 'ToJSON'\/'FromJSON' unchanged when we're not the one touching it
  -- matters more here than convenience.
  , lsLeaseTransitions :: !(Maybe Int)
  }
  deriving (Show, Eq)

instance FromJSON LeaseSpec where
  parseJSON = withObject "LeaseSpec" $ \o ->
    LeaseSpec
      <$> o .:? "holderIdentity"
      <*> o .:? "leaseDurationSeconds"
      <*> o .:? "renewTime"
      <*> o .:? "leaseTransitions"

instance ToJSON LeaseSpec where
  toJSON s =
    object
      [ "holderIdentity" .= lsHolderIdentity s
      , "leaseDurationSeconds" .= lsLeaseDurationSeconds s
      , "renewTime" .= lsRenewTime s
      , "leaseTransitions" .= lsLeaseTransitions s
      ]

data Lease = Lease
  { leaseMeta :: !ObjectMeta
  , leaseSpec :: !LeaseSpec
  }
  deriving (Show, Eq)

instance FromJSON Lease where
  parseJSON = withObject "Lease" $ \o -> Lease <$> o .: "metadata" <*> o .: "spec"

instance ToJSON Lease where
  toJSON l =
    object
      [ "apiVersion" .= apiVersionOf (resourceGVK (Nothing :: Maybe Lease))
      , "kind" .= gvkKind (resourceGVK (Nothing :: Maybe Lease))
      , "metadata" .= leaseMeta l
      , "spec" .= leaseSpec l
      ]

instance Resource Lease where
  resourceGVK _ = GVK "coordination.k8s.io" "v1" "Lease"
  resourceScope _ = Namespaced
  resourcePlural _ = "leases"
  resourceMeta = leaseMeta
  resourceSetMeta m x = x {leaseMeta = m}

-- --------------------------------------------------------------------------
-- Election
-- --------------------------------------------------------------------------

data LeaderElectionConfig = LeaderElectionConfig
  { lecLeaseName :: !Text
  , lecNamespace :: !Text
  , lecIdentity :: !Text
  -- ^ Must be unique per running process — typically the Pod name
  -- (@$(POD_NAME)@ via the downward API).
  , lecLeaseDuration :: !NominalDiffTime
  -- ^ How long a lease is valid without renewal before another candidate
  -- may take it.
  , lecRenewDeadline :: !NominalDiffTime
  -- ^ How often the current leader renews. Should be comfortably less
  -- than 'lecLeaseDuration' (client-go's own default ratio is 2:3).
  , lecRetryPeriod :: !NominalDiffTime
  -- ^ How often a non-leader checks whether the lease has become available.
  }

defaultLeaderElectionConfig :: Text -> Text -> Text -> LeaderElectionConfig
defaultLeaderElectionConfig leaseName ns identity =
  LeaderElectionConfig
    { lecLeaseName = leaseName
    , lecNamespace = ns
    , lecIdentity = identity
    , lecLeaseDuration = 15
    , lecRenewDeadline = 10
    , lecRetryPeriod = 2
    }

-- | Blocks forever, running @onStartLeading@ for as long as (and only
-- while) this process holds the lease, calling @onStoppedLeading@ as soon
-- as it doesn't (lease lost to another candidate, renewal failed, or
-- @onStartLeading@ exited on its own — the last case is treated as a
-- reason to stop leading and go back to the "trying to (re)acquire" state,
-- not as this function returning).
--
-- If @onStartLeading@ throws, that exception propagates out of this
-- function too — "let it crash" applies here the same way it does to
-- 'Kubernetes.Operator.Manager.runManager': a Kubernetes-restarted process
-- re-runs leader election from scratch, which is the correct recovery
-- path, not something this function should paper over.
-- | The type of the tiny self-contained interpreter this module builds:
-- unlifts a 'Lease'-flavoured 'Eff' computation straight to 'IO'. Fully
-- concrete (not rank-2) — @run@ is a single, closed composition of
-- interpreters ending in 'runEff', so its argument type is necessarily the
-- one specific stack those interpreters produce; 'attempt' (which *is*
-- polymorphic over @es@, see its signature) simply specializes to this
-- exact stack wherever it's passed to @run@. Named as its own type synonym
-- purely so the helpers below that thread @run@ through don't have to
-- repeat it.
type RunLease = Eff '[KubeClient Lease, KubeWriter Lease, IOE] Bool -> IO Bool

runWithLeaderElection :: KubeConfig -> LeaderElectionConfig -> IO () -> IO () -> IO ()
runWithLeaderElection kubeConfig lec onStartLeading onStoppedLeading = do
  mgr <- newManagerFor kubeConfig
  let scope = WatchNamespace (lecNamespace lec)
      run :: RunLease
      run = runEff . runKubeWriterIO @Lease mgr kubeConfig scope . runKubeClientIO @Lease mgr kubeConfig scope
  loop run
  where
    key = ObjectKey (Just (lecNamespace lec)) (lecLeaseName lec)

    loop :: RunLease -> IO ()
    loop run = do
      acquired <- tryAcquireOrRenew run
      if not acquired
        then threadDelay (microsFor (lecRetryPeriod lec)) >> loop run
        else do
          Async.withAsync onStartLeading $ \leaderAsync ->
            () <$ Async.race (Async.wait leaderAsync) (renewLoop run)
          onStoppedLeading
          loop run

    renewLoop :: RunLease -> IO ()
    renewLoop run = do
      threadDelay (microsFor (lecRenewDeadline lec))
      stillLeader <- tryAcquireOrRenew run
      if stillLeader then renewLoop run else pure ()

    tryAcquireOrRenew :: RunLease -> IO Bool
    tryAcquireOrRenew run = do
      now <- getCurrentTime
      result <- try (run (attempt now))
      case result of
        Left (e :: SomeException) -> do
          hPutStrLn stderr ("leader election: " <> show e)
          pure False
        Right ok -> pure ok

    attempt :: (KubeClient Lease :> es, KubeWriter Lease :> es, IOE :> es) => UTCTime -> Eff es Bool
    attempt now = do
      mLease <- getResource key
      case mLease of
        Nothing -> do
          _ <- createResource (freshLease now)
          pure True
        Just lease
          | isUs (leaseSpec lease) || isExpired now (leaseSpec lease) -> do
              _ <- updateResource (lease {leaseSpec = claim now})
              pure True
          | otherwise -> pure False

    isUs sp = lsHolderIdentity sp == Just (lecIdentity lec)

    isExpired now sp = case (lsRenewTime sp >>= parseRenewTime, lsLeaseDurationSeconds sp) of
      (Just rt, Just dur) -> diffUTCTime now rt > fromIntegral dur
      _ -> True -- malformed or missing: safest to treat as up for grabs

    claim now =
      LeaseSpec
        { lsHolderIdentity = Just (lecIdentity lec)
        , lsLeaseDurationSeconds = Just (round (lecLeaseDuration lec))
        , lsRenewTime = Just (formatRenewTime now)
        , lsLeaseTransitions = Just 0 -- not tracked precisely; cosmetic field only
        }

    freshLease now =
      Lease
        { leaseMeta = ObjectMeta (lecLeaseName lec) (Just (lecNamespace lec)) Nothing Nothing Nothing []
        , leaseSpec = claim now
        }

    microsFor :: NominalDiffTime -> Int
    microsFor d = max 0 (round (realToFrac d * 1000000 :: Double))

parseRenewTime :: Text -> Maybe UTCTime
parseRenewTime = parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" . T.unpack

formatRenewTime :: UTCTime -> Text
formatRenewTime = T.pack . formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%6QZ"
