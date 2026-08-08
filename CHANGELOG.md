# Revision history for kube-hs

## 0.1.0.0 -- YYYY-mm-dd

* First version. Released on an unsuspecting world.
* `csMaxRetries`/`crsMaxRetries` on a `ControllerSpec(RW)` is now enforced:
  a key whose consecutive `TransientError` count reaches the budget is given
  up on instead of retried forever (previously advisory).
* `ocResyncPeriod` in `OperatorConfig` is now honoured by the Reflector: the
  watch is time-limited to that period, so a silently-dropped watch
  notification is healed by a periodic re-LIST even when the stream never
  ends.
* New GitHub Actions CI (`.github/workflows/ci.yml`) runs `cabal build all`
  + `cabal test` on GHC 9.10 for every push and pull request.
* Three new worked examples filling the gaps in the existing set:
  `node-watcher` (read-only operator for a cluster-scoped built-in kind),
  `configmap-backup` (the write path + owner references against a built-in
  kind), and `service-deployer` (a CRD reconciler that creates and owns a
  Deployment via a hand-rolled cross-kind interpreter stack).
* DX: `Kubernetes.Resource.TH`'s `deriveResource` generates a `Resource`
  instance from one line, and `done`/`requeueAfter`/`requeueNow`/
  `transientError`/`permanentError` in `Kubernetes.Operator.Types` spell a
  reconciler as prose. Both are re-exported by `Kubernetes.Operator`.
* The worked examples now use that DX: `configmap-logger`, `pod-watcher`,
  `node-watcher`, `configmap-backup`, `leader-elected-configmap-logger`
  and `service-deployer` generate their `Resource` instances with
  `deriveResource`, and the reconcilers return results with the `done`/
  `requeueAfter`/`permanentError` helpers.
