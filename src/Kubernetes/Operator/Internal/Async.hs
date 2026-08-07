-- | Internal, not part of the public API. Shared by
-- "Kubernetes.Operator.Controller" and "Kubernetes.Operator.Manager" —
-- extracted here rather than left in one and imported by the other to
-- avoid a module cycle (Manager depends on Controller for
-- 'Kubernetes.Operator.Controller.CompiledController').
module Kubernetes.Operator.Internal.Async
  ( withAsyncs
  ) where

import Control.Concurrent.Async (Async)
import qualified Control.Concurrent.Async as Async

-- | 'Control.Concurrent.Async.withAsync' generalized to a list: structured
-- concurrency (every child is guaranteed cancelled if the body throws or
-- returns) without pulling in a dependency just for this.
withAsyncs :: [IO a] -> ([Async a] -> IO b) -> IO b
withAsyncs = go []
  where
    go acc [] k = k (reverse acc)
    go acc (io : ios) k = Async.withAsync io (\a -> go (a : acc) ios k)
