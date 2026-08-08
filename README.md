# kube-hs

A Haskell **library** for writing Kubernetes operators — a typed,
`effectful`-based alternative to `client-go`/`controller-runtime` and
`kopf`/`kubebuilder`, not a standalone application.

Everything you actually depend on lives under `src/Kubernetes/`, exposed as
one library target (`kube-hs`) in `kube-hs.cabal`. The `executable` stanzas
in that same file (`configmap-logger`, `website-operator`,
`leader-election-demo`, `website-webhook`, `node-watcher`,
`configmap-backup`, `service-deployer`, `crd-codegen`, ...) are not the
product — they're runnable examples under `examples/`, each demonstrating
one feature of the library end to end, kept in the same repo (and same
`cabal test`/CI) so they can't silently drift out of date. A real operator
you write is its own package that depends on `kube-hs`, with exactly one
`main` — shaped like whichever example is closest to what you're building.

Every feature below was built against, and verified against, a real `kind`
cluster — not just unit-tested against fakes.

## What's here

- **A typed extension point, not a hardcoded resource.** Implement one
  `Resource` instance (`resourceGVK`/`resourceScope`/`resourcePlural`/
  `resourceMeta`) for Pod, ConfigMap, or your own CRD type, and every layer
  above it — cache, workqueue, controller, manager — works for it
  unchanged. See `Kubernetes.Resource`.
- **Informer-style Cache + Workqueue + Reflector**, the same three pieces
  `client-go` is built on: a local, thread-safe read cache
  (`Kubernetes.Operator.Cache`), a rate-limited deduplicating work queue
  (`Kubernetes.Operator.Workqueue`), and the LIST-then-watch loop that
  keeps both in sync — auto-resyncing on `410 Gone`, and re-LISTing on a
  fixed `ocResyncPeriod` so a watch notification the client silently
  dropped is healed even when the stream never ends
  (`Kubernetes.Operator.Reflector`).
- **Controller + Manager**: wire a reconciler to a resource kind
  (`Kubernetes.Operator.Controller`), then run one or many controllers —
  for different resource kinds, side by side — with graceful
  SIGTERM/Ctrl-C shutdown (`Kubernetes.Operator.Manager`).
- **Finalizers** for cleanup-before-delete (`Kubernetes.Operator.Finalizer`).
- **Owner references and garbage collection** — set the right
  `ownerReferences`; the real API server's built-in GC controller does the
  actual cascading delete (`Kubernetes.Operator.OwnerReference`).
- **Leader election** via a `coordination.k8s.io/v1` Lease, compatible with
  Go operators using the same primitive (`Kubernetes.Operator.LeaderElection`).
- **Prometheus metrics**: counters and histograms served over a real
  `/metrics` HTTP endpoint (`Kubernetes.Operator.Metrics`).
- **Admission webhooks** (validating + mutating), served over TLS, decoded
  against your typed `Resource` (`Kubernetes.Operator.Webhook`).
- **CRD → Haskell codegen**: turn a CustomResourceDefinition's OpenAPI v3
  schema into a `Resource` instance and JSON codecs
  (`Kubernetes.Codegen.Schema`/`Generate`, `crd-codegen` CLI).
- The original minimal Pod list+watch client this grew out of is still
  there, untouched, as `Kubernetes.Client` / `exe/Main.hs` (`cabal run kube-hs`).

## Design in one paragraph

The `KubeClient`/`KubeWriter`/`Cache`/`Workqueue`/`Metrics` effects are all
kept strictly first-order dynamic effects
(`Effectful.Dispatch.Dynamic.interpret`/`send`) — no callbacks flowing back
into `Eff` — which is exactly the case `effectful` handles simply, without
the interpreter-composition ceremony of mtl transformer stacks or the
higher-order-effect complexity of something like `polysemy`. A reconciler
is written against a constraint (`Ctx a es` / `CtxRW a es`), not a concrete
monad stack, so adding a capability later (this is exactly what happened
when `Metrics` was added) never touches existing reconcilers. See the
Haddock on `Kubernetes.Operator.Controller.Ctx` for the details.

## Quickstart

A complete, real operator — watches ConfigMaps and logs their keys — is
about 50 lines. This is `examples/ConfigMapLogger.hs` with comments
trimmed; run it for real with `cabal run configmap-logger`:

```haskell
{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Data.Aeson (FromJSON (..), withObject, (.:), (.:?))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Effectful (Eff)
import Kubernetes.Operator

-- 1. The type, and the one instance the whole framework needs from it.
data ConfigMap = ConfigMap { cmMeta :: ObjectMeta, cmData :: Map T.Text T.Text }

instance FromJSON ConfigMap where
  parseJSON = withObject "ConfigMap" $ \o ->
    ConfigMap <$> o .: "metadata" <*> (fromMaybe Map.empty <$> o .:? "data")

instance Resource ConfigMap where
  resourceGVK _ = GVK "" "v1" "ConfigMap"
  resourceScope _ = Namespaced
  resourcePlural _ = "configmaps"
  resourceMeta = cmMeta
  resourceSetMeta m cm = cm { cmMeta = m }

-- 2. The reconciler: read current state from the Cache, react.
reconcile :: (Ctx ConfigMap es) => Request -> Eff es (Either ReconcileError ReconcileResult)
reconcile (Request key) = do
  mCm <- cacheGet @ConfigMap key
  case mCm of
    Nothing -> logInfo (renderKey key <> " deleted") >> pure (Right Done)
    Just cm -> logInfo (renderKey key <> " keys: " <> T.pack (show (Map.keys (cmData cm))))
                 >> pure (Right Done)

-- 3. Wire it up and run.
main :: IO ()
main = do
  kubeConfig <- loadKubeConfig
  let spec :: ControllerSpec ConfigMap
      spec = ControllerSpec
        { csName = "configmap-logger", csScope = WatchAllNamespaces
        , csWorkers = 4, csMaxRetries = 5, csReconcile = reconcile
        }
  metrics <- newMetricsRegistry
  controller <- compileController kubeConfig defaultOperatorConfig metrics spec
  withMetricsServer 9090 metrics (runManager defaultManagerConfig [controller])
```

Everything above `main` never mentions HTTP, JSON watch streams, retries,
or threads — that's the whole point of the layering. Need to *write* the
object back (status updates, finalizers, owner references)? Use
`ControllerSpecRW`/`compileControllerWithWriter` instead — see
`examples/WebsiteOperator.hs`.

## Connecting to a cluster

Every example uses `loadKubeConfig`, which tries, in order:

1. `KUBE_API_SERVER` (+ `KUBE_TOKEN`, `KUBE_CA_FILE`) if set — talk to a
   specific API server directly.
2. An in-cluster ServiceAccount, if
   `/var/run/secrets/kubernetes.io/serviceaccount/token` exists (i.e.
   running as a real Pod) — reads the token, CA bundle, and
   `KUBERNETES_SERVICE_HOST`/`_PORT` automatically. Just make sure the
   ServiceAccount has RBAC for whatever it lists/watches/writes.
3. Otherwise, `http://127.0.0.1:8001` — i.e. run `kubectl proxy --port=8001`
   in another terminal. This is the simplest way to try any example
   against a real cluster with zero auth/TLS setup.

| Variable                          | Meaning                                                        |
|------------------------------------|-----------------------------------------------------------------|
| `KUBE_API_SERVER`                  | Base URL, e.g. `https://1.2.3.4:6443` (skips in-cluster/proxy autodetection) |
| `KUBE_TOKEN`                       | Bearer token (used with `KUBE_API_SERVER`)                     |
| `KUBE_CA_FILE`                     | PEM CA bundle to verify the API server's certificate            |
| `KUBE_INSECURE_SKIP_TLS_VERIFY=1`  | Skip TLS verification entirely (testing only)                   |
| `KUBE_NAMESPACE`                   | Namespace to list/watch/write (default: `default`)              |
| `KUBE_ALL_NAMESPACES=1`            | Operate cluster-wide instead of one namespace                   |

For quick, throwaway testing against a cluster with a self-signed
certificate you don't want to source the CA for, `KUBE_INSECURE_SKIP_TLS_VERIFY=1`
skips verification — do **not** do this against anything you care about.

## Package layout

| Module | What it is |
|---|---|
| `Kubernetes.Client` | The original minimal Pod-only client (`Log`/`KubeClient` effects, auth/TLS setup) that everything else grew from — still a working, standalone example. |
| `Kubernetes.Resource` | `Resource` typeclass, `ObjectMeta`/`OwnerReference`/`GVK`/`ObjectKey` — the generic vocabulary every other module is written against. |
| `Kubernetes.Operator` | Public facade — `import` this, not the individual modules below, for normal use. |
| `Kubernetes.Operator.Client` | Generalized `KubeClient a`/`KubeWriter a` effects: list/get/watch, create/update/updateStatus, for any `Resource a`. |
| `Kubernetes.Operator.Cache` | The informer's local read cache. |
| `Kubernetes.Operator.Workqueue` | Rate-limited, deduplicating work queue (client-go style). |
| `Kubernetes.Operator.Reflector` | LIST + watch loop feeding the Cache and Workqueue; auto-resyncs on stream end or `410 Gone`. |
| `Kubernetes.Operator.Controller` | `Ctx`/`CtxRW` constraints, `ControllerSpec(RW)`, `compileController(WithWriter)`. |
| `Kubernetes.Operator.Manager` | Runs one or more compiled controllers; graceful SIGTERM/Ctrl-C shutdown. |
| `Kubernetes.Operator.Finalizer` | `ensureFinalizer` / `finalizeAndRemove`. |
| `Kubernetes.Operator.OwnerReference` | `setControllerReference` / `ensureControllerReference` and friends. |
| `Kubernetes.Operator.LeaderElection` | `Lease`-based `runWithLeaderElection`. |
| `Kubernetes.Operator.Metrics` | `incCounter`/`observeSeconds` effect + real Prometheus exporter. |
| `Kubernetes.Operator.Webhook` | `AdmissionReview` protocol, typed validating/mutating handlers, TLS server. |
| `Kubernetes.Codegen.Schema` / `Generate` | CRD YAML → `Resource` instance + JSON codecs (used by the `crd-codegen` executable). |

## Examples

Each is a complete `main`, runnable with `cabal run <name>` against
`kubectl proxy` (see above) or a real cluster.

| `cabal run ...` | Demonstrates |
|---|---|
| `kube-hs` | The original minimal Pod list+watch client (`Kubernetes.Client` directly, no operator framework). |
| `pod-watcher` | The same list+watch behavior rebuilt on the generic `Resource`/`Ctx` framework, read-only. |
| `node-watcher` | A read-only operator for a **cluster-scoped** built-in kind (Node) — where `resourceScope _ = ClusterScoped` and `WatchScope` is ignored. |
| `configmap-logger` | The quickstart above: one read-only `Controller`, metrics endpoint. |
| `leader-elected-configmap-logger` | The same reconciler wrapped in `runWithLeaderElection` — run two copies with different `IDENTITY` env vars to watch them hand off. |
| `leader-election-demo` | Leader election in isolation, no controller — just the Lease handoff. |
| `configmap-backup` | The minimal **write** operator: `compileControllerWithWriter`, `createResource`/`updateResource`, owner references / garbage collection against a built-in kind. |
| `website-operator` | The comprehensive example: a generated CRD type, finalizers, status subresource writes, metrics — via `ControllerSpecRW`/`compileControllerWithWriter`. |
| `website-webhook` | Validating + mutating admission webhooks for the same CRD, served over TLS. |
| `service-deployer` | A **cross-kind** operator: a CRD reconciler that creates and owns a Deployment (the documented hand-rolled-interpreter escape hatch). |
| `crd-codegen` | The codegen CLI itself (see below). |

## Generating Haskell types from a CRD

```
cabal run crd-codegen -- examples/crds/website-crd.yaml Website examples/generated/Website.hs
```

reads a `CustomResourceDefinition` manifest (YAML or JSON), picks a served
version's `openAPIV3Schema`, and writes a Haskell module with one record
type per object-shaped schema node, `FromJSON`/`ToJSON` instances, and a
`Resource` instance wired to the CRD's group/version/kind/plural — see
`examples/generated/Website.hs`, generated from `examples/crds/website-crd.yaml`
this way. The output is **not** regenerated automatically and is meant to
be hand-edited afterwards (add derived instances, helper functions, etc.);
if you do regenerate, diff first.

The `service-deployer` example's CRD type is instead hand-written (no
codegen) and lives inline in the example; a matching manifest to install it
against a cluster is `examples/crds/service-crd.yaml`.

## Admission webhooks

```haskell
app :: Wai.Application
app req respond = case Wai.pathInfo req of
  ["validate"] -> validatingWebhookApp (validateTyped myValidator) req respond
  ["mutate"]   -> mutatingWebhookApp (mutateTyped myMutator) req respond
  _            -> respond (Wai.responseLBS HTTP.status404 [] "not found")

main :: IO ()
main = runWebhookServerTLS 8443 "server.crt" "server.key" app
```

Kubernetes requires webhook endpoints to be served over TLS and refuses to
call one whose certificate doesn't validate against how it's addressed —
this library serves TLS but deliberately doesn't generate or manage the
certificate itself (in a real cluster that's normally cert-manager's job).
For local testing against a `kind` cluster, generate a self-signed cert
whose SAN matches the **DNS name** of the Service you put in front of it
(not its ClusterIP — this stack's certificate validation, like most TLS
libraries, only matches Subject Alternative Names against hostnames, never
against a dialed IP address):

```
openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
  -keyout server.key -out server.crt \
  -subj "/CN=website-webhook.default.svc" \
  -addext "subjectAltName=DNS:website-webhook.default.svc,DNS:website-webhook.default.svc.cluster.local"
```

then point a `ValidatingWebhookConfiguration`/`MutatingWebhookConfiguration`
at a Service in front of the Pod running `website-webhook server.crt
server.key`, with `caBundle` set to that same certificate (self-signed, so
it's its own CA here) and `clientConfig.service.name` matching the SAN
above exactly.

## Metrics

```haskell
metrics <- newMetricsRegistry
controller <- compileController kubeConfig defaultOperatorConfig metrics spec
withMetricsServer 9090 metrics (runManager defaultManagerConfig [controller])
```

Create **one** `MetricsRegistry` per process and pass it to every
`compileController`/`compileControllerWithWriter` call — a Prometheus
scrape target is one `/metrics` endpoint per process, not one per
controller. Reconcilers call `incCounter`/`observeSeconds` (see
`examples/WebsiteOperator.hs`); nothing about a reconciler changes based on
which interpreter (`runMetricsIO` for print-debugging,
`runMetricsPrometheus` for real) ends up running it.

Any executable calling `withMetricsServer`/`runMetricsServer` needs
`ghc-options: -threaded` in its own cabal stanza — Warp's I/O manager
requires GHC's threaded runtime, and a library has no way to impose that
flag on whatever depends on it.

## Owner references and garbage collection

```haskell
child' <- case setControllerReference owner child of
  Left err     -> ...            -- owner has no UID yet, or a conflicting controller exists
  Right child' -> updateResource child'
```

or, from inside a reconciler that already has `KubeWriter Child :> es`:

```haskell
_ <- ensureControllerReference owner child   -- like ensureFinalizer: True means "stop, requeue"
```

This library never deletes anything itself — setting `ownerReferences`
correctly is the whole job. The real API server's built-in
garbage-collector controller does the actual cascading delete, entirely
server-side, once the owner is gone.

## Leader election

```haskell
lec <- pure (defaultLeaderElectionConfig "my-operator-leader" "default" identity)
runWithLeaderElection kubeConfig lec
  (runManager defaultManagerConfig [controller])   -- while leading
  (putStrLn "lost leadership")                     -- once it isn't anymore
```

Uses a `coordination.k8s.io/v1` Lease — the same primitive client-go's
`leaderelection` package (and hence most real-world Go operators) is built
on, so this can safely run alongside, or be migrated from, one written in
Go against the same Lease.

## Build & test

```
cabal build all
cabal test
```

The test suite (`test/Spec.hs`) runs entirely against fake HTTP servers on
ephemeral ports (`Network.Wai.Handler.Warp`) — no external cluster or fixed
port required, safe to run concurrently and in CI. GitHub Actions runs
`cabal build all` + `cabal test` on GHC 9.10 for every push and pull request
(`.github/workflows/ci.yml`). Every feature above was additionally validated
by hand against a real `kind` cluster during development; that validation
isn't automated (it needs a running cluster, `kubectl`, and Docker), but
nothing shipped without it.

## Known limitations (intentionally out of scope, for now)

- Writes are full-object PUT with optimistic-concurrency conflict
  detection (via `resourceVersion`), not Server-Side Apply — a reconciler
  that only wants to own a few fields of an object shared with other
  writers has to implement that itself.
- One `KubeWriter a` per interpreted controller stack: a reconciler that
  creates/owns objects of a *different* kind (e.g. a CR that manages a
  child ConfigMap) needs to run that kind's own `KubeWriter`/`KubeClient`
  interpreter directly (see how `Kubernetes.Operator.OwnerReference`'s
  real-cluster validation does this) rather than getting it for free
  through `compileControllerWithWriter`.
- No informer-level field/label selectors — a controller always
  lists/watches every object of its kind in scope and filters in the
  reconciler if needed.
