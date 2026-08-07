-- | Basic observability, as a first-order dynamic effect in the same style
-- as 'Kubernetes.Client.Log'. Wired into every controller by default (see
-- 'Kubernetes.Operator.Controller.Ctx') since it's cheap and every operator
-- benefits from it, unlike the opt-in 'Kubernetes.Operator.Client.KubeWriter'.
--
-- 'runMetricsIO' just prints; a real deployment swaps it for a
-- Prometheus-backed interpreter (a @prometheus-client@ 'Counter'\/
-- 'Histogram' registered once and incremented\/observed here) without
-- touching a single reconciler — that swap is the entire point of the
-- effect boundary.
module Kubernetes.Operator.Metrics
  ( Metrics
  , incCounter
  , observeSeconds
  , runMetricsIO
  , runMetricsNoOp
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Effectful
import Effectful.Dispatch.Dynamic (interpret, send)

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
