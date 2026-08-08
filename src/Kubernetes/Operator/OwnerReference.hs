-- | Owner references and garbage collection. Like
-- "Kubernetes.Operator.Finalizer", this isn't a new effect — just pure
-- helpers over 'OwnerReference' plus one small 'KubeWriter'-based one,
-- following the same shape:
--
-- 1. When you create or update an object that's logically owned by
--    another (a child ConfigMap\/Deployment\/etc. a CR's reconciler
--    manages), call 'setControllerReference' before writing it, or
--    'ensureControllerReference' if you're updating an object you already
--    persisted once.
-- 2. That's it — actually deleting the dependent when the owner goes away
--    is the real API server's built-in garbage-collector controller's job,
--    entirely server-side. This library only ever writes the
--    @ownerReferences@ field correctly; it never deletes anything itself.
module Kubernetes.Operator.OwnerReference
  ( controllerRef
  , ownerRef
  , hasOwnerReference
  , findControllerRef
  , isControlledBy
  , setControllerReference
  , setOwnerReference
  , ensureControllerReference
  ) where

import Data.Aeson (ToJSON)
import Data.List (find)
import Data.Text (Text)
import Effectful
import Kubernetes.Operator.Client (KubeWriter, updateResource)
import Kubernetes.Operator.Types (ReconcileError (..))
import Kubernetes.Resource
  ( ObjectMeta (omName, omOwnerReferences, omUid)
  , OwnerReference (..)
  , Resource (..)
  , apiVersionOf
  , gvkKind
  , renderKey
  )

-- | Build the 'OwnerReference' that would make @owner@ the /managing/
-- controller of some other object (@controller = Just True@,
-- @blockOwnerDeletion = Just True@ — the same defaults
-- @controller-runtime@'s @SetControllerReference@ uses). Fails if @owner@
-- has no UID yet, i.e. it's a value constructed client-side that hasn't
-- actually been created (or freshly read) server-side — an owner
-- reference is meaningless without the real UID the API server assigns.
controllerRef :: (Resource owner) => owner -> Either Text OwnerReference
controllerRef = mkRef (Just True) (Just True)

-- | As 'controllerRef', but for a non-controlling owner reference
-- (@controller = Nothing@) — rarer, but the right thing for "this object
-- should be garbage-collected along with that one, but isn't managed by
-- it" (multiple such references can coexist on one object; controlling
-- ones cannot).
ownerRef :: (Resource owner) => owner -> Either Text OwnerReference
ownerRef = mkRef Nothing Nothing

mkRef :: (Resource owner) => Maybe Bool -> Maybe Bool -> owner -> Either Text OwnerReference
mkRef controller blockDeletion owner =
  case omUid meta of
    Nothing ->
      Left
        ( gvkKindOf owner
            <> " "
            <> renderKey (resourceKey owner)
            <> " has no UID yet — it must be created (or read back from the API server) before it can own anything"
        )
    Just uid ->
      Right
        OwnerReference
          { orApiVersion = apiVersionOf (resourceGVK (Just owner))
          , orKind = gvkKindOf owner
          , orName = omName meta
          , orUid = uid
          , orController = controller
          , orBlockOwnerDeletion = blockDeletion
          }
  where
    meta = resourceMeta owner

gvkKindOf :: (Resource owner) => owner -> Text
gvkKindOf owner = gvkKind (resourceGVK (Just owner))

-- | Does this metadata already carry the given owner reference (matched on
-- @apiVersion@\/@kind@\/@name@\/@uid@ — the fields that together identify
-- the /specific/ owner object, ignoring the two boolean flags)?
hasOwnerReference :: OwnerReference -> ObjectMeta -> Bool
hasOwnerReference ref meta = any (sameOwner ref) (omOwnerReferences meta)

sameOwner :: OwnerReference -> OwnerReference -> Bool
sameOwner a b = orApiVersion a == orApiVersion b && orKind a == orKind b && orName a == orName b && orUid a == orUid b

-- | The single owner reference with @controller = Just True@, if any —
-- mirrors the real API server's convention that an object has at most one
-- managing controller.
findControllerRef :: ObjectMeta -> Maybe OwnerReference
findControllerRef meta = find ((== Just True) . orController) (omOwnerReferences meta)

isControlledBy :: (Resource owner) => owner -> ObjectMeta -> Bool
isControlledBy owner meta = case controllerRef owner of
  Left _ -> False
  Right ref -> hasOwnerReference ref meta

-- | Set @owner@ as @child@'s managing controller, returning the updated
-- value to write back. Fails (without modifying @child@) if @owner@ has no
-- UID yet (see 'controllerRef'), or if @child@ already has a /different/
-- controller — the same conflict @controller-runtime@'s
-- @SetControllerReference@ refuses to silently overwrite, since exactly
-- one controller should ever be responsible for a given object.
setControllerReference :: (Resource owner, Resource child) => owner -> child -> Either Text child
setControllerReference owner child = do
  ref <- controllerRef owner
  case findControllerRef (resourceMeta child) of
    Just existing
      | sameOwner existing ref -> Right child -- already correct, no-op
      | otherwise ->
          Left
            ( gvkKindOf child
                <> " "
                <> renderKey (resourceKey child)
                <> " is already controlled by "
                <> orKind existing
                <> "/"
                <> orName existing
            )
    Nothing -> Right (addRef ref child)

-- | As 'setControllerReference', but for a non-controlling reference (see
-- 'ownerRef') — several of these can coexist, so this only ever adds
-- (idempotently; a byte-for-byte-identical reference already present is
-- left alone), never conflicts.
setOwnerReference :: (Resource owner, Resource child) => owner -> child -> Either Text child
setOwnerReference owner child = do
  ref <- ownerRef owner
  pure $
    if hasOwnerReference ref (resourceMeta child)
      then child
      else addRef ref child

addRef :: (Resource child) => OwnerReference -> child -> child
addRef ref child = resourceSetMeta meta' child
  where
    meta = resourceMeta child
    meta' = meta {omOwnerReferences = ref : omOwnerReferences meta}

-- | Reconciler-friendly wrapper around 'setControllerReference': if
-- @child@ doesn't yet carry @owner@'s controller reference, persists the
-- change with a PUT and reports 'True' — like 'Kubernetes.Operator.Finalizer.ensureFinalizer',
-- the caller should treat this as "stop here and requeue", since the write
-- just made the in-hand copy of @child@ stale. Reports 'False' with no
-- write if it was already set. A UID-missing or conflicting-controller
-- failure surfaces as a 'PermanentError' — retrying without a spec or
-- cluster-state change wouldn't help either case.
ensureControllerReference
  :: (Resource owner, Resource child, ToJSON child, KubeWriter child :> es)
  => owner
  -> child
  -> Eff es (Either ReconcileError Bool)
ensureControllerReference owner child
  | isControlledBy owner (resourceMeta child) = pure (Right False)
  | otherwise = case setControllerReference owner child of
      Left err -> pure (Left (PermanentError err))
      Right child' -> do
        _ <- updateResource child'
        pure (Right True)
