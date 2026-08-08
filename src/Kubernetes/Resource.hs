-- | Generic resource identity — the pieces of a Kubernetes object that the
-- cache, workqueue, and controller machinery need to know about without
-- caring whether the object is a Pod, a ConfigMap, or a CRD.
--
-- This is deliberately separate from JSON decoding ('FromJSON'): a
-- 'Resource' instance answers "what kind is this and how do I address it?";
-- 'FromJSON' (defined per-type as usual) answers "how do I parse it?". The
-- generalized 'KubeClient' interpreter (roadmap step 1) will require both.
module Kubernetes.Resource
  ( GVK (..)
  , apiVersionOf
  , Scope (..)
  , WatchScope (..)
  , ObjectKey (..)
  , renderKey
  , ObjectMeta (..)
  , OwnerReference (..)
  , Resource (..)
  , IntOrString (..)
  , envelopeParseJSON
  , envelopeToJSON
  ) where

import Data.Aeson
  ( FromJSON (..)
  , ToJSON (..)
  , Value
  , object
  , withObject
  , (.:)
  , (.:?)
  , (.=)
  )
import Control.Applicative ((<|>))
import Data.Aeson.Types (Parser)
import Data.Maybe (catMaybes, fromMaybe, maybeToList)
import Data.Text (Text)

-- | Group/Version/Kind. Group is @""@ for core/v1 types (Pod, ConfigMap, ...).
data GVK = GVK
  { gvkGroup :: !Text
  , gvkVersion :: !Text
  , gvkKind :: !Text
  }
  deriving (Show, Eq)

-- | The @apiVersion@ field's wire format: @group\/version@, or just
-- @version@ for the core group (@gvkGroup = ""@, e.g. @v1@ for ConfigMap).
apiVersionOf :: GVK -> Text
apiVersionOf (GVK grp ver _)
  | grp == "" = ver
  | otherwise = grp <> "/" <> ver

-- | Whether a kind lives under @/namespaces/{ns}/...@ or is cluster-scoped.
-- Fixed per-type (an instance property); see 'WatchScope' for the
-- per-Controller choice of *which* namespace(s) to actually watch.
data Scope = Namespaced | ClusterScoped
  deriving (Show, Eq)

-- | Which namespace(s) a particular Controller watches. Deliberately kept
-- separate from 'Kubernetes.Client.KubeConfig' (which only describes how to
-- reach the cluster: URL, auth, TLS) rather than folded into it — a single
-- connection is commonly shared by several controllers watching different
-- namespaces (or, for a 'ClusterScoped' resource, no namespace at all, in
-- which case this is ignored).
data WatchScope = WatchAllNamespaces | WatchNamespace !Text
  deriving (Show, Eq)

-- | The stable identity of an object: what you'd use as a workqueue item
-- and a cache key. Namespace is 'Nothing' for cluster-scoped resources.
data ObjectKey = ObjectKey
  { okNamespace :: !(Maybe Text)
  , okName :: !Text
  }
  deriving (Show, Eq, Ord)

renderKey :: ObjectKey -> Text
renderKey (ObjectKey Nothing n) = n
renderKey (ObjectKey (Just ns) n) = ns <> "/" <> n

-- | The subset of every object's @metadata@ the generic machinery cares
-- about. Enough for caching, keying, finalizers, and ownership —
-- deliberately not a full copy of every possible metadata field
-- (labels\/annotations are still missing; add them here if a reconciler
-- needs them, following the same pattern).
data ObjectMeta = ObjectMeta
  { omName :: !Text
  , omNamespace :: !(Maybe Text)
  , omResourceVersion :: !(Maybe Text)
  , omUid :: !(Maybe Text)
  , omDeletionTimestamp :: !(Maybe Text)
  , omFinalizers :: ![Text]
  , omOwnerReferences :: ![OwnerReference]
  }
  deriving (Show, Eq)

instance FromJSON ObjectMeta where
  parseJSON = withObject "ObjectMeta" $ \o ->
    ObjectMeta
      <$> o .: "name"
      <*> o .:? "namespace"
      <*> o .:? "resourceVersion"
      <*> o .:? "uid"
      <*> o .:? "deletionTimestamp"
      <*> (fromMaybe [] <$> o .:? "finalizers")
      <*> (fromMaybe [] <$> o .:? "ownerReferences")

-- | Only needed for write operations (see
-- 'Kubernetes.Operator.Client.KubeWriter'); read-only controllers never
-- touch this. @resourceVersion@ is included so a PUT built from a value
-- read out of the Cache carries the optimistic-concurrency token the API
-- server expects — omitting or staling it is exactly what turns into a 409
-- Conflict.
instance ToJSON ObjectMeta where
  toJSON m =
    object $
      [ "name" .= omName m
      , "finalizers" .= omFinalizers m
      , "ownerReferences" .= omOwnerReferences m
      ]
        ++ catMaybes
          [ ("namespace" .=) <$> omNamespace m
          , ("resourceVersion" .=) <$> omResourceVersion m
          , ("uid" .=) <$> omUid m
          , ("deletionTimestamp" .=) <$> omDeletionTimestamp m
          ]

-- | A back-reference recorded on a /dependent/ object pointing at the
-- /owner/ object responsible for it — the mechanism the real API server's
-- built-in garbage collector uses to decide what to delete when the owner
-- goes away. This library never deletes anything itself: setting this
-- field correctly (see "Kubernetes.Operator.OwnerReference") is the whole
-- job, since the cluster's own garbage-collector controller does the
-- actual cascading delete, entirely server-side, once the owner is gone.
data OwnerReference = OwnerReference
  { orApiVersion :: !Text
  , orKind :: !Text
  , orName :: !Text
  , orUid :: !Text
  , orController :: !(Maybe Bool)
  -- ^ 'Just True' marks this as the /managing/ controller — at most one
  -- owner reference should set this, mirroring the real API server's
  -- (loosely enforced, client-side-conventional) rule that an object has
  -- at most one controller.
  , orBlockOwnerDeletion :: !(Maybe Bool)
  -- ^ 'Just True' asks the API server to refuse deleting the owner via the
  -- foreground policy until this dependent is gone too. Client-set (the
  -- API server only /honours/ it, it doesn't compute it), so it's just
  -- another field this library writes — not something it enforces itself.
  }
  deriving (Show, Eq)

instance FromJSON OwnerReference where
  parseJSON = withObject "OwnerReference" $ \o ->
    OwnerReference
      <$> o .: "apiVersion"
      <*> o .: "kind"
      <*> o .: "name"
      <*> o .: "uid"
      <*> o .:? "controller"
      <*> o .:? "blockOwnerDeletion"

instance ToJSON OwnerReference where
  toJSON r =
    object $
      [ "apiVersion" .= orApiVersion r
      , "kind" .= orKind r
      , "name" .= orName r
      , "uid" .= orUid r
      ]
        ++ catMaybes
          [ ("controller" .=) <$> orController r
          , ("blockOwnerDeletion" .=) <$> orBlockOwnerDeletion r
          ]

-- | One instance per Kind you want to list/watch/reconcile — Pod,
-- ConfigMap, or a CRD type you define yourself. This is the extension
-- point that makes the operator framework not-Pod-specific: everything
-- above the HTTP layer (Cache, Workqueue, Controller) is written purely
-- against this class plus 'FromJSON', never against a concrete type.
class Resource a where
  resourceGVK :: proxy a -> GVK
  resourceScope :: proxy a -> Scope

  -- | REST path fragment used to build the collection URL, e.g. @"pods"@
  -- or @"configmaps"@ (or, for a CRD, its plural name).
  resourcePlural :: proxy a -> Text

  resourceMeta :: a -> ObjectMeta

  -- | The setter half of 'resourceMeta'. Only needed for writes (see
  -- "Kubernetes.Operator.Finalizer", which uses it to persist a modified
  -- finalizer list) — read-only reconcilers never call it. Almost always
  -- just a record update, e.g. @resourceSetMeta m cm = cm { cmMeta = m }@.
  resourceSetMeta :: ObjectMeta -> a -> a

  resourceKey :: a -> ObjectKey
  resourceKey x = ObjectKey (omNamespace m) (omName m)
    where
      m = resourceMeta x

-- | The Kubernetes API's @x-kubernetes-int-or-string@ convention (used for
-- things like container ports and percentage-or-absolute values): the wire
-- value is either a JSON number or a JSON string, and callers are expected
-- to accept either. One shared type rather than per-CRD duplicates.
data IntOrString = IOSInt !Int | IOSString !Text
  deriving (Show, Eq)

instance FromJSON IntOrString where
  parseJSON v = (IOSInt <$> parseJSON v) <|> (IOSString <$> parseJSON v)

instance ToJSON IntOrString where
  toJSON (IOSInt n) = toJSON n
  toJSON (IOSString s) = toJSON s

-- | The common shape of a full Kubernetes object's JSON: fixed
-- @apiVersion@\/@kind@ (from its 'GVK', not stored redundantly on the
-- Haskell value), @metadata@, @spec@, and an optional @status@. Most
-- hand-written or generated 'FromJSON'\/'ToJSON' instances can be built
-- directly from these instead of repeating the envelope shape per type —
-- see the generated code from @crd-codegen@ for the intended usage.
envelopeParseJSON :: (FromJSON spec, FromJSON status) => Value -> Parser (ObjectMeta, spec, Maybe status)
envelopeParseJSON = withObject "Kubernetes object" $ \o ->
  (,,) <$> o .: "metadata" <*> o .: "spec" <*> o .:? "status"

envelopeToJSON :: (ToJSON spec, ToJSON status) => GVK -> ObjectMeta -> spec -> Maybe status -> Value
envelopeToJSON g m s mSt =
  object $
    [ "apiVersion" .= apiVersionOf g
    , "kind" .= gvkKind g
    , "metadata" .= m
    , "spec" .= s
    ]
      ++ maybeToList (("status" .=) <$> mSt)
