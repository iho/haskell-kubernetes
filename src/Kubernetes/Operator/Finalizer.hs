-- | Finalizer handling, built on the opt-in 'KubeWriter' effect. Not a new
-- effect of its own — just two small, composable functions a reconciler
-- calls at the right points, following the same shape as every
-- controller-runtime-family framework:
--
-- 1. On every reconcile of a live object, call 'ensureFinalizer' first. If
--    it wasn't already present, it just got added (persisted with a PUT)
--    and the object you're holding is now stale — stop here (just 'done';
--    no need to schedule your own retry, see 'ensureFinalizer''s Haddock).
-- 2. If 'isBeingDeleted', skip your normal logic entirely and call
--    'finalizeAndRemove' with your cleanup action instead.
--
-- See "Kubernetes.Operator.Controller" for 'CtxRW', the constraint that
-- makes 'KubeWriter' available to a reconciler alongside the usual 'Ctx'.
module Kubernetes.Operator.Finalizer
  ( isBeingDeleted
  , hasFinalizer
  , ensureFinalizer
  , finalizeAndRemove
  ) where

import Data.Aeson (ToJSON)
import Data.Maybe (isJust)
import Data.Text (Text)
import Effectful
import Kubernetes.Operator.Client (KubeWriter, updateResource)
import Kubernetes.Operator.Types (ReconcileError)
import Kubernetes.Resource (ObjectMeta (omDeletionTimestamp, omFinalizers), Resource (..))

-- | True once the API server has recorded a deletion request. From this
-- point the object is only still visible because at least one finalizer
-- (maybe not yours) hasn't been removed yet — actually deleting it happens
-- the instant the finalizer list becomes empty, entirely server-side.
isBeingDeleted :: (Resource a) => a -> Bool
isBeingDeleted obj = isJust (omDeletionTimestamp (resourceMeta obj))

hasFinalizer :: (Resource a) => Text -> a -> Bool
hasFinalizer name obj = name `elem` omFinalizers (resourceMeta obj)

-- | Call at the top of a reconciler for an object that is /not/ being
-- deleted. Adds the given finalizer name and persists the change if it
-- isn't there yet, returning 'True' — the caller should stop here (e.g.
-- 'Kubernetes.Operator.Types.done'), not keep going with the in-hand
-- object, since it's now stale. No need to schedule an explicit retry
-- either: the PUT's resulting @MODIFIED@ watch event is what the Reflector
-- feeds back into the Cache and Workqueue, so this key naturally comes back
-- around on its own once that lands — requeuing by hand as well would just
-- race it, adding a redundant reconcile in the (typical) case where the
-- watch event wins. Returns 'False' with no side effect if the finalizer
-- was already present.
ensureFinalizer :: (Resource a, ToJSON a, KubeWriter a :> es) => Text -> a -> Eff es Bool
ensureFinalizer name obj
  | hasFinalizer name obj = pure False
  | otherwise = do
      let meta = resourceMeta obj
          meta' = meta {omFinalizers = name : omFinalizers meta}
      _ <- updateResource (resourceSetMeta meta' obj)
      pure True

-- | Call instead of normal reconcile logic once 'isBeingDeleted'. Runs the
-- cleanup action; if it succeeds (or the finalizer wasn't present to begin
-- with — already cleaned up, or never added), removes the finalizer and
-- persists that, which is what actually lets Kubernetes finish deleting
-- the object once every other finalizer is also gone. If the cleanup
-- action fails, the finalizer is deliberately left in place so the object
-- can't disappear before cleanup has actually succeeded — the whole point
-- of a finalizer.
finalizeAndRemove
  :: (Resource a, ToJSON a, KubeWriter a :> es)
  => Text
  -> a
  -> Eff es (Either ReconcileError ())
  -> Eff es (Either ReconcileError ())
finalizeAndRemove name obj cleanup
  | not (hasFinalizer name obj) = pure (Right ())
  | otherwise = do
      result <- cleanup
      case result of
        Left err -> pure (Left err)
        Right () -> do
          let meta = resourceMeta obj
              meta' = meta {omFinalizers = filter (/= name) (omFinalizers meta)}
          _ <- updateResource (resourceSetMeta meta' obj)
          pure (Right ())
