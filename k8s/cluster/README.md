# Phase 2 cluster bring-up

Single-node kubeadm control plane on the Ubuntu box (`ssh intern-fatima`). Rationale for
every choice here lives in SPEC.md → "Phase 2 cluster"; this file is just the runbook.

Run in order. Everything is idempotent except `kubeadm init`.

## 1. Host preflight

```sh
sudo swapoff -a && sudo sed -i.bak '/\sswap\s/s/^/#/' /etc/fstab
printf 'overlay\nbr_netfilter\n' | sudo tee /etc/modules-load.d/k8s.conf
sudo modprobe overlay && sudo modprobe br_netfilter
printf 'net.bridge.bridge-nf-call-iptables  = 1\nnet.bridge.bridge-nf-call-ip6tables = 1\nnet.ipv4.ip_forward                 = 1\n' \
  | sudo tee /etc/sysctl.d/k8s.conf
sudo sysctl --system
```

## 2. containerd

Docker already installs containerd but ships it with `disabled_plugins = ["cri"]`, so the
default config is regenerated rather than a second runtime being installed. The Docker
original is preserved at `/etc/containerd/config.toml.docker-orig`.

```sh
sudo cp /etc/containerd/config.toml /etc/containerd/config.toml.docker-orig
sudo sh -c 'containerd config default > /etc/containerd/config.toml'
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
sudo systemctl restart containerd
```

Check `sandbox = 'registry.k8s.io/pause:X'` in the generated config matches
`kubeadm config images list | grep pause`.

## 3. kubeadm / kubelet / kubectl

```sh
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.35/deb/Release.key \
  | sudo gpg --dearmor --yes -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.35/deb/ /' \
  | sudo tee /etc/apt/sources.list.d/kubernetes.list
sudo apt-get update && sudo apt-get install -y kubelet kubeadm kubectl
sudo apt-mark hold kubelet kubeadm kubectl   # no surprise minor bumps
```

## 4. Cluster

```sh
sudo kubeadm init --config kubeadm-config.yaml
mkdir -p ~/.kube && sudo cp -f /etc/kubernetes/admin.conf ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config && chmod 600 ~/.kube/config

# Single node: nothing schedules unless the control-plane taint comes off.
kubectl taint nodes --all node-role.kubernetes.io/control-plane-
```

## 5. Calico

CRDs are a separate manifest in 3.3x and must go first, or the `Installation` CR fails with
`no matches for kind "Installation"`.

```sh
kubectl create -f https://raw.githubusercontent.com/projectcalico/calico/v3.32.1/manifests/operator-crds.yaml
kubectl create -f https://raw.githubusercontent.com/projectcalico/calico/v3.32.1/manifests/tigera-operator.yaml
kubectl -n tigera-operator rollout status deploy/tigera-operator
kubectl create -f calico-installation.yaml
kubectl get tigerastatus -w      # all AVAILABLE=True, then the node goes Ready
```

## 6. MetalLB + ingress-nginx

```sh
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.16.0/config/manifests/metallb-native.yaml
kubectl -n metallb-system rollout status deploy/controller
kubectl apply -f metallb-pool.yaml

kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.15.1/deploy/static/provider/cloud/deploy.yaml
kubectl -n ingress-nginx rollout status deploy/ingress-nginx-controller
```

## 7. Make LoadBalancer IPs reachable off-box

Without this, `10.50.0.240` answers only from the box — see SPEC.md for why.

```sh
sudo tailscale set --advertise-routes=10.50.0.240/28
```

Then **approve the route** at <https://login.tailscale.com/admin/machines> →
`aayush-internserver` → Subnet routes. Verify from a laptop:

```sh
curl -I http://10.50.0.240/     # 404 from nginx default backend == working
```

## 8. kubectl from a laptop

`100.64.0.9` is already a cert SAN, so only the server URL needs rewriting:

```sh
ssh intern-fatima 'sudo cat /etc/kubernetes/admin.conf' > /tmp/admin.conf
sed -i '' 's|https://10.50.0.2:6443|https://100.64.0.9:6443|' /tmp/admin.conf
KUBECONFIG=~/.kube/config:/tmp/admin.conf kubectl config view --flatten > /tmp/merged
cp ~/.kube/config ~/.kube/config.bak && install -m 600 /tmp/merged ~/.kube/config
rm -f /tmp/admin.conf /tmp/merged
kubectl config use-context streaming-k8s
```

## Verify / tear down the smoke test

`smoke-test.yaml` exercises scheduling, Calico, CoreDNS, MetalLB and both the L7 (Ingress)
and L4 (`type: LoadBalancer`) paths — the latter being the shape MediaMTX's RTMP Service
will take.

```sh
kubectl apply -f smoke-test.yaml
curl -s -H 'Host: whoami.local' http://10.50.0.240/            # L7
curl -s http://$(kubectl -n smoke-test get svc whoami-l4 \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}'):8080/    # L4

kubectl delete -f smoke-test.yaml
```
