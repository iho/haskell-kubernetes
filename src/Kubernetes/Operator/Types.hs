-- | Shared vocabulary for the operator layer: what a reconciler is handed,
-- what it can report back, and the small bit of static configuration every
-- controller needs.
module Kubernetes.Operator.Types
  ( Request (..)
  , ReconcileResult (..)
  , done
  , requeueAfter
  , requeueNow
  , ReconcileError (..)
  , transientError
  , permanentError
  , OperatorConfig (..)
  , defaultOperatorConfig
  ) where

import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Kubernetes.Resource (ObjectKey)

-- | What a reconciler is handed: just the object's key, controller-runtime
-- style — deliberately *not* the object itself.
--
-- Design decision: if 'Request' carried the object, a reconciler could act
-- on a stale copy handed to it by an old watch event that's since been
-- superseded (or arrive after the object was deleted). By carrying only the
-- key and requiring the reconciler to read the current state from the
-- 'Kubernetes.Operator.Cache.Cache' itself, reconciliation becomes
-- level-triggered: "something changed about X, go look at X's current
-- state and make it match" rather than "here's what changed, react to it".
-- That's what makes an operator self-healing after a missed event, a
-- duplicate delivery, or a restart with an empty queue but a fresh LIST.
newtype Request = Request {requestKey :: ObjectKey}
  deriving (Show, Eq)

-- | What a reconciler asks the controller to do next, on success.
data ReconcileResult
  = -- | Nothing more to do until the next watch event or resync.
    Done
  | -- | Try again after a delay (e.g. "come back once this Job should have finished").
    RequeueAfter !NominalDiffTime
  | -- | Try again as soon as a worker is free (rare — usually you want 'Done'
    -- and to rely on the watch to notice the next relevant change).
    RequeueImmediately
  deriving (Show, Eq)

-- | Explicit failure, not an exception: reconcilers are expected to *return*
-- errors so the controller can decide how to react (retry with backoff vs.
-- give up), rather than relying on exception handling for ordinary control
-- flow. See 'Kubernetes.Operator.Controller.workerLoop' for how each case is
-- handled, and its Haddock for the (deliberately narrow) role exceptions
-- still play as a worker-loop safety net.
data ReconcileError
  = -- | Might succeed if retried — a conflict, a transient network error,
    -- a dependency that isn't ready yet. Retried with exponential backoff.
    TransientError !Text
  | -- | Retrying would never help (e.g. the object's spec is invalid).
    -- Logged and dropped; a future spec change will re-trigger reconciliation.
    PermanentError !Text
  deriving (Show, Eq)

-- ---------------------------------------------------------------------------
-- Small constructors, so a reconciler reads as plain prose rather than
-- repeating the full @Right Done@ / @Left (TransientError msg)@ spelling.
-- ---------------------------------------------------------------------------

-- | @'pure' ('Right' 'Done')@ — nothing more to do until the next watch
-- event or resync. The overwhelmingly common case in a reconciler's happy
-- path.
done :: (Applicative f) => f (Either ReconcileError ReconcileResult)
done = pure (Right Done)

-- | @'pure' ('Right' ('RequeueAfter' d))@ — try again after a delay.
requeueAfter :: (Applicative f) => NominalDiffTime -> f (Either ReconcileError ReconcileResult)
requeueAfter d = pure (Right (RequeueAfter d))

-- | @'pure' ('Right' 'RequeueImmediately')@ — try again as soon as a worker
-- is free.
requeueNow :: (Applicative f) => f (Either ReconcileError ReconcileResult)
requeueNow = pure (Right RequeueImmediately)

-- | @'Left' ('TransientError' msg)@ — failed, but might succeed if retried.
transientError :: (Applicative f) => Text -> f (Either ReconcileError ReconcileResult)
transientError = pure . Left . TransientError

-- | @'Left' ('PermanentError' msg)@ — retrying would never help.
permanentError :: (Applicative f) => Text -> f (Either ReconcileError ReconcileResult)
permanentError = pure . Left . PermanentError

-- | Static, rarely-changing configuration threaded via effectful's static
-- 'Effectful.Reader.Static.Reader' effect rather than a bespoke effect of
-- our own — this is exactly the case that effect is for.
newtype OperatorConfig = OperatorConfig
  { ocResyncPeriod :: NominalDiffTime
  -- ^ Full re-LIST interval even absent watch events, to heal from any
  -- watch notification the client silently dropped. Implemented by the
  -- Reflector (roadmap step 2/6), not by any effect interpreter here.
  }
  deriving (Show)

defaultOperatorConfig :: OperatorConfig
defaultOperatorConfig = OperatorConfig {ocResyncPeriod = 10 * 60}
