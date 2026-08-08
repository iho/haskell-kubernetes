-- | A deliberately partial model of OpenAPI v3 schemas — just enough to
-- describe the shapes real Kubernetes CRDs use in their
-- @spec.versions[].schema.openAPIV3Schema@. Unsupported constructs
-- (@oneOf@\/@anyOf@\/@allOf@, @$ref@, numeric\/string format constraints,
-- ...) degrade to 'SAny' rather than failing the whole generator — a CRD
-- author can always tighten the generated type by hand afterwards, but a
-- codegen tool that refuses to run at all on a schema it doesn't fully
-- understand is much less useful than one that does its best.
module Kubernetes.Codegen.Schema
  ( Schema (..)
  , CrdDef (..)
  , parseCrd
  ) where

import Data.Aeson
  ( FromJSON (..)
  , Value (..)
  , withObject
  , (.:)
  , (.:?)
  )
import Data.Aeson.Types (Parser, parseEither)
import qualified Data.Map.Strict as Map
import Data.Map.Strict (Map)
import Data.Maybe (fromMaybe)
import Data.Text (Text)

-- | The subset of a schema's shape this generator understands.
data Schema
  = SObject !(Map Text Schema) ![Text]
  -- ^ Properties and their names, and which of those names are required.
  | SMap !Schema
  -- ^ @additionalProperties@ with no (or empty) @properties@ — a
  -- string-keyed map, e.g. arbitrary labels.
  | SArray !Schema
  | SString !(Maybe [Text])
  -- ^ 'Nothing': any string. 'Just' vs: an enum restricted to these values.
  | SInteger
  | SNumber
  | SBoolean
  | SIntOrString
  -- ^ @x-kubernetes-int-or-string@: wire value is a JSON number or string.
  | SAny
  -- ^ Anything not modeled above — decoded as raw 'Data.Aeson.Value'.
  deriving (Show, Eq)

-- | Everything pulled out of a @CustomResourceDefinition@ manifest needed
-- to generate a 'Kubernetes.Resource.Resource' instance plus spec\/status
-- types: identity (group\/kind\/plural\/scope), which version's schema to
-- generate from, and that schema split into its @spec@ and @status@ parts.
data CrdDef = CrdDef
  { crdGroup :: !Text
  , crdKind :: !Text
  , crdPlural :: !Text
  , crdScopeNamespaced :: !Bool
  , crdVersionName :: !Text
  , crdSpecSchema :: !Schema
  , crdStatusSchema :: !(Maybe Schema)
  }
  deriving (Show)

-- --------------------------------------------------------------------------
-- Parsing a raw OpenAPI schema Value into 'Schema'
-- --------------------------------------------------------------------------

-- | Mirrors the handful of OpenAPI v3 keywords this generator understands,
-- decoded loosely (every field optional) since real-world schemas are
-- inconsistent about which of these are present.
data RawSchema = RawSchema
  { rsType :: !(Maybe Text)
  , rsProperties :: !(Maybe (Map Text RawSchema))
  , rsRequired :: !(Maybe [Text])
  , rsItems :: !(Maybe RawSchema)
  , rsEnum :: !(Maybe [Text])
  , rsAdditionalProperties :: !(Maybe AdditionalPropertiesRaw)
  , rsIntOrString :: !(Maybe Bool)
  }

data AdditionalPropertiesRaw = APBool !Bool | APSchema !RawSchema

instance FromJSON RawSchema where
  parseJSON = withObject "OpenAPI schema" $ \o ->
    RawSchema
      <$> o .:? "type"
      <*> o .:? "properties"
      <*> o .:? "required"
      <*> o .:? "items"
      <*> o .:? "enum"
      <*> o .:? "additionalProperties"
      <*> o .:? "x-kubernetes-int-or-string"

instance FromJSON AdditionalPropertiesRaw where
  parseJSON (Bool b) = pure (APBool b)
  parseJSON v = APSchema <$> parseJSON v

toSchema :: RawSchema -> Schema
toSchema rs
  | rsIntOrString rs == Just True = SIntOrString
  | otherwise = case rsType rs of
      Just "object" -> objectOrMap
      Nothing
        | hasProps || hasAdditional -> objectOrMap
      Just "array" -> SArray (maybe SAny toSchema (rsItems rs))
      Just "string" -> SString (rsEnum rs)
      Just "integer" -> SInteger
      Just "number" -> SNumber
      Just "boolean" -> SBoolean
      _ -> SAny
  where
    hasProps = maybe False (not . Map.null) (rsProperties rs)
    hasAdditional = case rsAdditionalProperties rs of
      Just (APBool True) -> True
      Just (APSchema _) -> True
      _ -> False

    objectOrMap
      | hasProps =
          SObject
            (Map.map toSchema (fromMaybe Map.empty (rsProperties rs)))
            (fromMaybe [] (rsRequired rs))
      | otherwise = case rsAdditionalProperties rs of
          Just (APSchema valueSchema) -> SMap (toSchema valueSchema)
          Just (APBool True) -> SMap SAny
          _ -> SAny

-- --------------------------------------------------------------------------
-- Pulling a CrdDef out of a whole CustomResourceDefinition manifest
-- --------------------------------------------------------------------------

-- | Picks the @served: true@ version if any are marked so, else the first
-- version listed — a CRD always has at least one.
parseCrd :: Value -> Either String CrdDef
parseCrd = parseEither $ withObject "CustomResourceDefinition" $ \top -> do
  spec <- top .: "spec"
  group <- spec .: "group"
  names <- spec .: "names"
  kind <- names .: "kind"
  plural <- names .: "plural"
  scopeText <- spec .:? "scope" :: Parser (Maybe Text)
  versions <- spec .: "versions"
  version <- pickVersion versions >>= withObject "version" pure
  versionName <- version .: "name"
  schemaWrapper <- version .: "schema"
  openApiSchema <- schemaWrapper .: "openAPIV3Schema"
  rawTop <- parseJSON openApiSchema :: Parser RawSchema
  let topSchema = toSchema rawTop
  (specSchema, statusSchema) <- case topSchema of
    SObject props _ ->
      pure
        ( fromMaybe (SObject Map.empty []) (Map.lookup "spec" props)
        , Map.lookup "status" props
        )
    _ -> pure (SAny, Nothing)
  pure
    CrdDef
      { crdGroup = group
      , crdKind = kind
      , crdPlural = plural
      , crdScopeNamespaced = fromMaybe "Namespaced" scopeText == "Namespaced"
      , crdVersionName = versionName
      , crdSpecSchema = specSchema
      , crdStatusSchema = statusSchema
      }
  where
    pickVersion :: [Value] -> Parser Value
    pickVersion [] = fail "CustomResourceDefinition has no versions"
    pickVersion vs@(v0 : _) = do
      served <- mapM (\v -> (,) v . fromMaybe False <$> parseServed v) vs
      pure (maybe v0 fst (lookupFirstTrue served))

    parseServed :: Value -> Parser (Maybe Bool)
    parseServed = withObject "version" (.:? "served")

    lookupFirstTrue :: [(a, Bool)] -> Maybe (a, Bool)
    lookupFirstTrue xs = case filter snd xs of
      (x : _) -> Just x
      [] -> Nothing
