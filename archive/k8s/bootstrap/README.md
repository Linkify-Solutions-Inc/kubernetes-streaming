# k8s/bootstrap

The only directory in this repo that a human applies by hand.

```sh
kubectl apply -f k8s/bootstrap/root-app.yaml
```

That is it. One command, once, after ArgoCD itself is installed with Helm
(ArgoCD cannot bootstrap itself — something has to install the thing that
installs everything else).

`root-app.yaml` is an ArgoCD `Application` whose source is
`k8s/argocd/apps/` — a directory of *other* `Application` manifests. Argo syncs
that directory, creating the six child Applications, and each of those syncs
its own directory. Adding a component to the cluster from this point on is a
pull request that adds a file, never a `kubectl`.

Full walkthrough, including the sync-wave ordering that makes operators land
before the resources that need them: `docs/aws/15-argocd-gitops.md`.

## Undoing it

```sh
# Hands the cluster back cleanly — the finalizer cascades to every child.
kubectl delete -f k8s/bootstrap/root-app.yaml

# Leaves everything running but stops Argo managing it (useful for debugging).
kubectl patch app root -n argocd --type merge \
  -p '{"metadata":{"finalizers":null}}' && \
kubectl delete app root -n argocd --cascade=orphan
```
