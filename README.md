# kube-hs

A minimal, direct-style Kubernetes client in Haskell using
[`effectful`](https://github.com/haskell-effectful/effectful). It lists Pods,
then watches them for changes, printing every `ADDED`/`MODIFIED`/`DELETED`/
`BOOKMARK`/`ERROR` event until the stream ends or you press Ctrl-C.

## Layout

- `src/Kubernetes/Client.hs` — everything effect-related: the `Log` and
  `KubeClient` effects, their real (HTTP) interpreters, the JSON types for
  Pods/PodLists/watch events, and `program` (the pure, effect-polymorphic
  orchestration: list, then loop over watch events).
- `exe/Main.hs` — the IO shell: figures out how to authenticate
  (in-cluster ServiceAccount, explicit token+CA, or `kubectl proxy`), builds
  the right `http-client` `Manager` (TLS+CA / TLS insecure / TLS system
  store / plain HTTP), and runs `program` with top-level handling for
  `KubeApiError` (esp. `410 Gone`) and Ctrl-C.

### Why not raw `http2`?

The task prefers HTTP/2 where practical. Kubernetes' watch stream is
comfortably served over HTTP/1.1 chunked transfer encoding, and driving the
`http2` package directly for a client this size (manual frame/stream
bookkeeping, flow control, trailers) would add a lot of code without
changing behavior against a real API server, since `http-client`/TLS
already negotiate the best protocol the server offers and Kubernetes API
servers happily serve watches over HTTP/1.1. `http-client` + `http-client-tls`
was chosen instead for robustness and to keep the example focused on the
effect design and the watch/streaming logic. Swapping the interpreter
(`runKubeClientIO`) for one built on `http2` would not require touching
`program` at all — that's the point of the effect boundary.

### Why `effectful`

The `KubeClient` effect is kept strictly first-order (`ListPods`,
`OpenWatch`, `NextEvent`, `CloseWatch` — no callbacks flowing back into
`Eff`), which is exactly the case `effectful`'s dynamic dispatch
(`Effectful.Dispatch.Dynamic.interpret`/`send`) handles simply, with none of
the interpreter-composition ceremony of mtl transformer stacks and none of
the higher-order-effect complexity you'd hit with something like `polysemy`.
Resource cleanup for the watch connection is just `Effectful.Exception.finally`,
which works exactly like `Control.Exception.finally` and is guaranteed to
run even under the asynchronous `UserInterrupt` exception GHC's runtime
delivers on Ctrl-C.

## Build

```
cabal build
```

## Run against `kubectl proxy` (simplest — no auth/TLS to configure)

```
kubectl proxy --port=8001
```

In another terminal:

```
cabal run kube-hs
```

By default the app talks to `http://127.0.0.1:8001` in the `default`
namespace. Set `KUBE_NAMESPACE=kube-system` to change namespace, or
`KUBE_ALL_NAMESPACES=1` to watch Pods across all namespaces.

## Run against a real cluster

Point it at the API server directly with a bearer token:

```
export KUBE_API_SERVER=https://<api-server-host>:6443
export KUBE_TOKEN=$(kubectl create token default)   # or a ServiceAccount token you already have
export KUBE_CA_FILE=/path/to/ca.crt                  # cluster CA bundle, for TLS verification
export KUBE_NAMESPACE=default
cabal run kube-hs
```

For quick, throwaway testing against a cluster with a self-signed
certificate you don't want to source the CA for, you can skip verification
(do **not** do this against anything you care about):

```
export KUBE_INSECURE_SKIP_TLS_VERIFY=1
```

## Run in-cluster

No configuration needed: when `/var/run/secrets/kubernetes.io/serviceaccount/token`
exists (i.e. running as a Pod with a mounted ServiceAccount), the app reads
the token, CA bundle, and `KUBERNETES_SERVICE_HOST`/`KUBERNETES_SERVICE_PORT`
automatically. Just make sure the ServiceAccount has RBAC permission to
`list`/`watch` `pods` in the target namespace(s).

## Environment variables

| Variable                          | Meaning                                                        |
|------------------------------------|-----------------------------------------------------------------|
| `KUBE_API_SERVER`                  | Base URL, e.g. `https://1.2.3.4:6443` (skips in-cluster/proxy autodetection) |
| `KUBE_TOKEN`                       | Bearer token (used with `KUBE_API_SERVER`)                     |
| `KUBE_CA_FILE`                     | PEM CA bundle to verify the API server's certificate            |
| `KUBE_INSECURE_SKIP_TLS_VERIFY=1`  | Skip TLS verification entirely (testing only)                   |
| `KUBE_NAMESPACE`                   | Namespace to list/watch (default: `default`)                    |
| `KUBE_ALL_NAMESPACES=1`            | Watch Pods cluster-wide instead of one namespace                |

## Known limitations (intentionally out of scope)

- On `410 Gone` / "resourceVersion too old" the app logs the error and
  exits cleanly rather than automatically re-listing and resuming the
  watch — a real controller would loop back to LIST and reopen the watch.
- No client-side rate limiting/backoff, leader election, or informer-style
  local cache — this is a list+watch foundation, not a full operator
  framework.
