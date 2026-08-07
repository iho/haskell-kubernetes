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
  , Scope (..)
  , WatchScope (..)
  , ObjectKey (..)
  , renderKey
  , ObjectMeta (..)
  , Resource (..)
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.:?), (.=))
import Data.Maybe (catMaybes, fromMaybe)
import Data.Text (Text)

-- | Group/Version/Kind. Group is @""@ for core/v1 types (Pod, ConfigMap, ...).
data GVK = GVK
  { gvkGroup :: !Text
  , gvkVersion :: !Text
  , gvkKind :: !Text
  }
  deriving (Show, Eq)

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
-- about. Enough for caching, keying, and finalizers — deliberately not a
-- full copy of every possible metadata field (labels/annotations/owner
-- references are all still missing; add them here if a reconciler needs
-- them, following the same pattern).
data ObjectMeta = ObjectMeta
  { omName :: !Text
  , omNamespace :: !(Maybe Text)
  , omResourceVersion :: !(Maybe Text)
  , omUid :: !(Maybe Text)
  , omDeletionTimestamp :: !(Maybe Text)
  , omFinalizers :: ![Text]
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
      ]
        ++ catMaybes
          [ ("namespace" .=) <$> omNamespace m
          , ("resourceVersion" .=) <$> omResourceVersion m
          , ("uid" .=) <$> omUid m
          , ("deletionTimestamp" .=) <$> omDeletionTimestamp m
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
