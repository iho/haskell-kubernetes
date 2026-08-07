{-# LANGUAGE AllowAmbiguousTypes #-}

-- | A per-resource-kind, in-memory, thread-safe local cache — the "Store"
-- half of an informer. Kept as a small first-order dynamic effect (same
-- style as 'Kubernetes.Client.Log' / 'Kubernetes.Client.KubeClient') so
-- reconcilers can be tested against a fake in-memory cache without any
-- HTTP involved at all.
--
-- One 'CacheStore' (hence one call to 'runCacheIO') per Controller: caches
-- are per resource-kind, not shared globally, matching one 'Cache' effect
-- per @a@ in the effect row.
module Kubernetes.Operator.Cache
  ( Cache
  , cacheGet
  , cacheList
  , cacheUpsert
  , cacheDelete
  , cacheReplace
  , cacheResourceVersion
  , CacheStore
  , newCacheStore
  , runCacheIO
  ) where

import Control.Concurrent.STM
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import Effectful
import Effectful.Dispatch.Dynamic (interpret, send)
import Kubernetes.Resource (ObjectKey, Resource (..))

data Cache a :: Effect where
  CacheGet :: ObjectKey -> Cache a m (Maybe a)
  CacheList :: Cache a m [a]
  CacheUpsert :: a -> Cache a m ()
  CacheDelete :: ObjectKey -> Cache a m ()
  -- | Full resync: replace every item and record the LIST's resourceVersion.
  CacheReplace :: [a] -> Text -> Cache a m ()
  CacheResourceVersion :: Cache a m (Maybe Text)

type instance DispatchOf (Cache a) = Dynamic

cacheGet :: (Cache a :> es) => ObjectKey -> Eff es (Maybe a)
cacheGet = send . CacheGet

cacheList :: (Cache a :> es) => Eff es [a]
cacheList = send CacheList

cacheUpsert :: (Cache a :> es) => a -> Eff es ()
cacheUpsert = send . CacheUpsert

cacheDelete :: forall a es. (Cache a :> es) => ObjectKey -> Eff es ()
cacheDelete key = send (CacheDelete key :: Cache a (Eff es) ())

cacheReplace :: (Cache a :> es) => [a] -> Text -> Eff es ()
cacheReplace items rv = send (CacheReplace items rv)

cacheResourceVersion :: forall a es. (Cache a :> es) => Eff es (Maybe Text)
cacheResourceVersion = send (CacheResourceVersion :: Cache a (Eff es) (Maybe Text))

data CacheStore a = CacheStore
  { csItems :: !(TVar (Map ObjectKey a))
  , csRV :: !(TVar (Maybe Text))
  }

newCacheStore :: IO (CacheStore a)
newCacheStore = CacheStore <$> newTVarIO Map.empty <*> newTVarIO Nothing

runCacheIO :: (Resource a, IOE :> es) => CacheStore a -> Eff (Cache a : es) r -> Eff es r
runCacheIO store = interpret $ \_ -> \case
  CacheGet key -> liftIO (Map.lookup key <$> readTVarIO (csItems store))
  CacheList -> liftIO (Map.elems <$> readTVarIO (csItems store))
  CacheUpsert x -> liftIO . atomically $ modifyTVar' (csItems store) (Map.insert (resourceKey x) x)
  CacheDelete key -> liftIO . atomically $ modifyTVar' (csItems store) (Map.delete key)
  CacheReplace items rv -> liftIO . atomically $ do
    writeTVar (csItems store) (Map.fromList [(resourceKey x, x) | x <- items])
    writeTVar (csRV store) (Just rv)
  CacheResourceVersion -> liftIO (readTVarIO (csRV store))
