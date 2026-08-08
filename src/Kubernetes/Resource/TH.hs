{-# LANGUAGE TemplateHaskell #-}

-- | Template Haskell to cut the per-kind boilerplate of a 'Resource'
-- instance. For the common case — a record type whose metadata lives in one
-- field, addressed by a fixed group\\/version\\/kind\\/plural — a whole
-- 'Kubernetes.Resource.Resource' instance is five one-line methods that say
-- nothing new. 'deriveResource' generates all five from a single call.
--
-- __Requires @{-# LANGUAGE TemplateHaskell, OverloadedStrings #-}@ in the
-- module that uses it__, so the generated string literals resolve to 'Text'
-- via the @'Data.String.IsString'@ instance (exactly the extension every
-- example already enables).
--
-- = Example
--
-- @
-- {-\# LANGUAGE OverloadedStrings, TemplateHaskell \#-}
-- data ConfigMap = ConfigMap { cmMeta :: 'Kubernetes.Resource.ObjectMeta', cmData :: ... }
--
-- deriveResource ''ConfigMap "" "v1" "ConfigMap" "configmaps" True 'cmMeta
-- @
--
-- generates the same 'Resource' instance you would have hand-written:
--
-- @
-- instance 'Kubernetes.Resource.Resource' ConfigMap where
--   resourceGVK _ = 'Kubernetes.Resource.GVK' \"\" \"v1\" \"ConfigMap\"
--   resourceScope _ = 'Kubernetes.Resource.Namespaced'
--   resourcePlural _ = \"configmaps\"
--   resourceMeta = cmMeta
--   resourceSetMeta m x = x { cmMeta = m }
-- @
module Kubernetes.Resource.TH
  ( deriveResource
  ) where

import Language.Haskell.TH
import Kubernetes.Resource
  ( GVK (GVK)
  , Resource (resourceGVK, resourceMeta, resourcePlural, resourceScope, resourceSetMeta)
  , Scope (ClusterScoped, Namespaced)
  )

-- | Generate a 'Resource' instance for @type@ addressed at the given
-- group\\/version\\/kind\\/plural, with @namespaced = True@ for a
-- namespaced kind (else cluster-scoped), and the record field
-- @metaField@ holding the 'Kubernetes.Resource.ObjectMeta'.
--
-- @resourceMeta@ becomes the field accessor itself; @resourceSetMeta@ becomes
-- the record update @\\m x -> x { metaField = m }@. So @metaField@ must be a
-- real record field of @type@, and the type must have @OverloadedStrings@
-- available at the splice site.
deriveResource
  :: Name -- ^ the record type, e.g. @''ConfigMap@
  -> String -- ^ API group (@\"\"@ for core\\/v1 types like Pod or ConfigMap)
  -> String -- ^ API version, e.g. @\"v1\"@ or @\"v1beta1\"@
  -> String -- ^ kind, e.g. @\"ConfigMap\"@
  -> String -- ^ REST plural, e.g. @\"configmaps\"@
  -> Bool -- ^ 'True' for 'Kubernetes.Resource.Namespaced', 'False' for cluster-scoped
  -> Name -- ^ the metadata record field, e.g. @'cmMeta@
  -> Q [Dec]
deriveResource ty group version kind plural namespaced metaField = do
  let scopeCon = if namespaced then 'Namespaced else 'ClusterScoped
      lit = LitE . StringL
      gvk = foldl AppE (ConE 'GVK) [lit group, lit version, lit kind]
      m = mkName "m"
      x = mkName "x"
      methods =
        [ FunD 'resourceGVK [Clause [WildP] (NormalB gvk) []]
        , FunD 'resourceScope [Clause [WildP] (NormalB (ConE scopeCon)) []]
        , FunD 'resourcePlural [Clause [WildP] (NormalB (lit plural)) []]
        , FunD 'resourceMeta [Clause [] (NormalB (VarE metaField)) []]
        , FunD 'resourceSetMeta
            [ Clause [VarP m, VarP x] (NormalB (RecUpdE (VarE x) [(metaField, VarE m)])) []
            ]
        ]
  pure [InstanceD Nothing [] (AppT (ConT ''Resource) (ConT ty)) methods]
