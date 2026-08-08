-- | CLI front-end for "Kubernetes.Codegen.Generate": read a
-- CustomResourceDefinition manifest (YAML or JSON), pick a served version's
-- schema, and write a Haskell module implementing 'Resource' for it.
--
-- > crd-codegen path/to/crd.yaml MyOperator.Types.MyApp src/MyOperator/Types/MyApp.hs
module Main (main) where

import qualified Data.ByteString as BS
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Yaml as Yaml
import Kubernetes.Codegen.Generate (generateModule)
import Kubernetes.Codegen.Schema (parseCrd)
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

main :: IO ()
main = do
  args <- getArgs
  case args of
    [crdPath, moduleName, outPath] -> run crdPath (T.pack moduleName) outPath
    _ -> do
      hPutStrLn stderr "usage: crd-codegen <crd.yaml> <ModuleName> <output.hs>"
      exitFailure

run :: FilePath -> Text -> FilePath -> IO ()
run crdPath moduleName outPath = do
  bytes <- BS.readFile crdPath
  value <- either (failWith . show) pure (Yaml.decodeEither' bytes)
  crd <- either failWith pure (parseCrd value)
  let source = generateModule moduleName crd
  TIO.writeFile outPath source
  putStrLn ("wrote " <> outPath)
  where
    failWith :: String -> IO a
    failWith msg = hPutStrLn stderr ("crd-codegen: " <> msg) >> exitFailure
