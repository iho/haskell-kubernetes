{-# LANGUAGE OverloadedStrings #-}

-- | Basic observability, as a first-order dynamic effect in the same style
-- as 'Kubernetes.Client.Log'. Wired into every controller by default (see
-- 'Kubernetes.Operator.Controller.Ctx') since it's cheap and every operator
-- benefits from it, unlike the opt-in 'Kubernetes.Operator.Client.KubeWriter'.
--
-- 'runMetricsIO' just prints and 'runMetricsNoOp' discards — both useful
-- for quick testing — but 'runMetricsPrometheus' is the one meant for a
-- real deployment: a thread-safe, in-process registry of named counters
-- and histograms, exposed as a real @\/metrics@ endpoint in Prometheus
-- text exposition format via 'metricsApp'\/'runMetricsServer'. Swapping
-- the interpreter is the whole point of the effect boundary — no
-- reconciler that calls 'incCounter'\/'observeSeconds' needs to change.
--
-- __Any executable calling 'runMetricsServer'\/'withMetricsServer' must add
-- @ghc-options: -threaded@ to its own cabal stanza.__ Warp's I\/O manager
-- requires the threaded runtime and fails at startup without it
-- (@getSystemTimerManager: the TimerManager requires linking against the
-- threaded runtime@) — this can only be set on the final executable, a
-- library has no way to impose it on its dependents, so it doesn't matter
-- that this module already depends on @warp@ under the hood.
module Kubernetes.Operator.Metrics
  ( Metrics
  , incCounter
  , observeSeconds
  , runMetricsIO
  , runMetricsNoOp

    -- * Real Prometheus exposition
  , MetricsRegistry
  , newMetricsRegistry
  , runMetricsPrometheus
  , renderPrometheus
  , metricsApp
  , runMetricsServer
  , withMetricsServer
  ) where

import qualified Control.Concurrent.Async as Async
import Control.Concurrent.STM
import qualified Data.ByteString.Lazy as BL
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Effectful
import Effectful.Dispatch.Dynamic (interpret, send)
import qualified Network.HTTP.Types as HTTP
import qualified Network.Wai as Wai
import qualified Network.Wai.Handler.Warp as Warp
import Numeric (showFFloat)

data Metrics :: Effect where
  IncCounter :: Text -> Metrics m ()
  ObserveSeconds :: Text -> Double -> Metrics m ()

type instance DispatchOf Metrics = Dynamic

incCounter :: (Metrics :> es) => Text -> Eff es ()
incCounter = send . IncCounter

observeSeconds :: (Metrics :> es) => Text -> Double -> Eff es ()
observeSeconds name secs = send (ObserveSeconds name secs)

runMetricsIO :: (IOE :> es) => Eff (Metrics : es) a -> Eff es a
runMetricsIO = interpret $ \_ -> \case
  IncCounter name -> liftIO (putStrLn ("[metric] counter " <> T.unpack name <> " += 1"))
  ObserveSeconds name secs -> liftIO (putStrLn ("[metric] histogram " <> T.unpack name <> " observed " <> show secs <> "s"))

-- | Discards every observation — for tests that don't care about metrics
-- and would rather not print noise.
runMetricsNoOp :: Eff (Metrics : es) a -> Eff es a
runMetricsNoOp = interpret $ \_ -> \case
  IncCounter _ -> pure ()
  ObserveSeconds _ _ -> pure ()

-- --------------------------------------------------------------------------
-- Real Prometheus exposition
-- --------------------------------------------------------------------------

-- | Matches the Prometheus client libraries' own conventional defaults
-- (seconds-scale buckets), since every current caller of 'observeSeconds'
-- times exactly that. Not configurable (yet) — the whole registry is
-- deliberately minimal; add per-metric bucket configuration if\/when a
-- caller actually needs a different scale.
defaultBuckets :: [Double]
defaultBuckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]

data Histogram = Histogram
  { histBuckets :: !(TVar (Map Double Int))
  -- ^ Bucket upper bound -> /cumulative/ count of observations @<=@ that
  -- bound (Prometheus's @le@ semantics) — updating every bucket @>=@ the
  -- observed value at observe-time means rendering is just "print what's
  -- stored," no cumulative-sum pass needed at scrape time.
  , histSum :: !(TVar Double)
  , histCount :: !(TVar Int)
  }

newHistogramSTM :: STM Histogram
newHistogramSTM =
  Histogram
    <$> newTVar (Map.fromList [(b, 0) | b <- defaultBuckets])
    <*> newTVar 0
    <*> newTVar 0

observeHistogramSTM :: Histogram -> Double -> STM ()
observeHistogramSTM h x = do
  modifyTVar' (histBuckets h) (Map.mapWithKey bump)
  modifyTVar' (histSum h) (+ x)
  modifyTVar' (histCount h) (+ 1)
  where
    bump bound count = if x <= bound then count + 1 else count

-- | A process-wide store of named counters and histograms, filled in by
-- 'runMetricsPrometheus' and read by 'metricsApp'\/'renderPrometheus'.
-- Create __one per process__ with 'newMetricsRegistry' and share it across
-- every 'Kubernetes.Operator.Controller.compileController' call — a
-- Prometheus scrape target is one @\/metrics@ endpoint per process, not
-- one per controller, the same way a real cluster only ever has one
-- kubelet-exposed @\/metrics@ regardless of how many containers a Pod runs.
data MetricsRegistry = MetricsRegistry
  { regCounters :: !(TVar (Map Text (TVar Int)))
  , regHistograms :: !(TVar (Map Text Histogram))
  }

newMetricsRegistry :: IO MetricsRegistry
newMetricsRegistry = MetricsRegistry <$> newTVarIO Map.empty <*> newTVarIO Map.empty

-- | Get-or-create-and-increment in a single transaction — safe under
-- concurrent workers incrementing the same counter name for the first
-- time simultaneously.
bumpCounter :: MetricsRegistry -> Text -> IO ()
bumpCounter reg name = atomically $ do
  counters <- readTVar (regCounters reg)
  case Map.lookup name counters of
    Just c -> modifyTVar' c (+ 1)
    Nothing -> do
      c <- newTVar 1
      modifyTVar' (regCounters reg) (Map.insert name c)

observeNamed :: MetricsRegistry -> Text -> Double -> IO ()
observeNamed reg name x = atomically $ do
  histograms <- readTVar (regHistograms reg)
  h <- case Map.lookup name histograms of
    Just h -> pure h
    Nothing -> do
      h <- newHistogramSTM
      modifyTVar' (regHistograms reg) (Map.insert name h)
      pure h
  observeHistogramSTM h x

-- | The real interpreter: same effect, same reconciler code, but every
-- 'incCounter'\/'observeSeconds' call now updates a live, scrapeable
-- registry instead of printing.
runMetricsPrometheus :: (IOE :> es) => MetricsRegistry -> Eff (Metrics : es) a -> Eff es a
runMetricsPrometheus reg = interpret $ \_ -> \case
  IncCounter name -> liftIO (bumpCounter reg name)
  ObserveSeconds name secs -> liftIO (observeNamed reg name secs)

-- | A single atomic snapshot of the whole registry, rendered as
-- Prometheus's text exposition format. One 'atomically' block for the
-- entire render, not one per metric, so a concurrent scrape never sees a
-- torn mix of before-and-after values across different metrics.
-- | Plain decimal notation (@0.005@, not @5.0e-3@) — 'show' on 'Double'
-- switches to scientific notation for small magnitudes, which is valid per
-- the exposition format's grammar but not what Prometheus's own client
-- libraries emit, and unnecessarily surprising to a human reading a scrape.
formatDouble :: Double -> Text
formatDouble d = T.pack (showFFloat Nothing d "")

renderPrometheus :: MetricsRegistry -> IO Text
renderPrometheus reg = atomically $ do
  counters <- readTVar (regCounters reg) >>= traverse readTVar
  histograms <- readTVar (regHistograms reg)
  histogramLines <- concat <$> mapM renderHistogram (Map.toList histograms)
  pure (T.unlines (concatMap renderCounter (Map.toList counters) ++ histogramLines))
  where
    renderCounter (name, v) =
      [ "# TYPE " <> name <> " counter"
      , name <> " " <> T.pack (show v)
      ]

    renderHistogram :: (Text, Histogram) -> STM [Text]
    renderHistogram (name, h) = do
      buckets <- readTVar (histBuckets h)
      total <- readTVar (histCount h)
      s <- readTVar (histSum h)
      pure $
        ["# TYPE " <> name <> " histogram"]
          ++ [ name <> "_bucket{le=\"" <> formatDouble bound <> "\"} " <> T.pack (show count)
             | (bound, count) <- Map.toAscList buckets
             ]
          ++ [ name <> "_bucket{le=\"+Inf\"} " <> T.pack (show total)
             , name <> "_sum " <> formatDouble s
             , name <> "_count " <> T.pack (show total)
             ]

-- | Serves the registry's current state at whatever path this is mounted
-- on — there's only one kind of response, so unlike
-- "Kubernetes.Operator.Webhook" this doesn't need to dispatch on
-- 'Wai.pathInfo' itself.
metricsApp :: MetricsRegistry -> Wai.Application
metricsApp reg _req respond = do
  body <- renderPrometheus reg
  respond
    ( Wai.responseLBS
        HTTP.status200
        [("Content-Type", "text/plain; version=0.0.4; charset=utf-8")]
        (BL.fromStrict (TE.encodeUtf8 body))
    )

-- | Plain HTTP, deliberately — unlike admission webhooks, Prometheus
-- scraping inside a cluster is conventionally plain HTTP (scrape configs
-- that need TLS are the exception, set up explicitly, not the default).
runMetricsServer :: Warp.Port -> MetricsRegistry -> IO ()
runMetricsServer port reg = Warp.run port (metricsApp reg)

-- | Runs @action@ (typically 'Kubernetes.Operator.Manager.runManager')
-- with the @\/metrics@ endpoint served alongside it for as long as it
-- runs. The metrics server is not linked to @action@'s fate in either
-- direction: if it crashes, that's a shame for scraping but not a reason
-- to take the operator down, and 'Control.Concurrent.Async.withAsync'
-- ensures it's cleaned up once @action@ returns either way.
withMetricsServer :: Warp.Port -> MetricsRegistry -> IO a -> IO a
withMetricsServer port reg action = Async.withAsync (runMetricsServer port reg) (const action)
