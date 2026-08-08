-- | Public facade for the operator framework. Import this module rather
-- than the individual @Kubernetes.Operator.*@ modules for normal use.
--
-- Deliberately does /not/ re-export the whole of "Kubernetes.Client":
-- that module's @KubeClient@\/@WatchEvent@ are the original Pod-only demo
-- effect and would clash with this module's generalized, resource-
-- parametrized versions of the same names from
-- "Kubernetes.Operator.Client". Only the pieces every operator author
-- needs regardless — logging and cluster connection config — are
-- re-exported explicitly below.
module Kubernetes.Operator
  ( module Kubernetes.Resource
  , module Kubernetes.Resource.TH
  , module Kubernetes.Operator.Types
  , module Kubernetes.Operator.Cache
  , module Kubernetes.Operator.Workqueue
  , module Kubernetes.Operator.Client
  , module Kubernetes.Operator.Reflector
  , module Kubernetes.Operator.Controller
  , module Kubernetes.Operator.Manager
  , module Kubernetes.Operator.Metrics
  , module Kubernetes.Operator.Finalizer
  , module Kubernetes.Operator.OwnerReference
  , module Kubernetes.Operator.LeaderElection

    -- * Reused from "Kubernetes.Client"
  , Log
  , LogLevel (..)
  , logInfo
  , logWarn
  , logErr
  , KubeConfig (..)
  , Namespace (..)
  , KubeApiError (..)
  , newManagerFor
  , loadKubeConfig
  ) where

import Kubernetes.Client
  ( KubeApiError (..)
  , KubeConfig (..)
  , Log
  , LogLevel (..)
  , Namespace (..)
  , loadKubeConfig
  , logErr
  , logInfo
  , logWarn
  , newManagerFor
  )
import Kubernetes.Operator.Cache
import Kubernetes.Operator.Client
import Kubernetes.Operator.Controller
import Kubernetes.Operator.Finalizer
import Kubernetes.Operator.OwnerReference
import Kubernetes.Operator.LeaderElection
import Kubernetes.Operator.Manager
import Kubernetes.Operator.Metrics
import Kubernetes.Operator.Reflector
import Kubernetes.Operator.Types
import Kubernetes.Operator.Workqueue
import Kubernetes.Resource
import Kubernetes.Resource.TH
