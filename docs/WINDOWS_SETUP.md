# Windows 10 Setup Guide

## Prerequisites

You need three things before running `setup_local_cluster.sh`:

### 1. Docker Desktop for Windows

Download: https://www.docker.com/products/docker-desktop/

Docker Desktop provides a lightweight Linux VM, the `docker` CLI, and a built-in Kubernetes cluster. Do not select "use Windows containers" during install — you want Linux containers.

After install:
1. Open Docker Desktop (you may need to update WSL first: PowerShell as admin → `wsl --update` → restart)
2. Go to **Settings → Kubernetes**
3. Check **Enable Kubernetes** (Kubeadm, single-node)
4. Click **Apply & Restart**
5. Wait for the green "Kubernetes running" indicator (bottom left)

**Resource allocation:** Docker Desktop on WSL2 manages resources dynamically. With 16GB system RAM you should be fine with defaults. If things are slow, allocate at least 2 CPU / 6 GB RAM via Settings → Resources.

### 2. kubectl

Usually comes with Docker Desktop. Test it:
```bash
kubectl version --client
```

If not found:
```powershell
winget install Kubernetes.kubectl
```

### 3. Helm

```powershell
winget install Helm.Helm
```

Or download directly from https://github.com/helm/helm/releases/latest — grab `helm-v3.x.x-windows-amd64.zip`, extract to `C:\tools\helm`, and add to PATH.

Test:
```bash
helm version
```

---

## Quick Start

After prerequisites are installed:

```bash
# In Git Bash, from the setup/ directory

# 1. Check everything is ready
./setup_local_cluster.sh check

# 2. Install Flink operator + example job (~3-5 min)
./setup_local_cluster.sh

# 3. In a SEPARATE Git Bash window, start port-forward
./setup_local_cluster.sh forward

# 4. Back in first window, test it (from scripts/ directory)
cd ../scripts
python flink_client.py
python 0_observe.py --stdout

# 5. When done
./setup_local_cluster.sh teardown
```

---

## What Gets Installed

| Component | Namespace | Purpose |
|-----------|-----------|---------|
| cert-manager | cert-manager | Required by Flink operator |
| flink-kubernetes-operator | flink-operator | Manages Flink deployments |
| basic-example | flink-test | Sample Flink job for testing |

All contained in your local Docker Desktop Kubernetes. Nothing touches cloud/GCP.

---

## Known Issues

### Flink operator YAML type error

The Flink operator expects all `flinkConfiguration` values as strings. The upstream example YAML uses an integer for `taskmanager.numberOfTaskSlots`, which causes a validation error:

```
The FlinkDeployment "basic-example" is invalid:
spec.flinkConfiguration.taskmanager.numberOfTaskSlots:
Invalid value: "integer": must be of type string
```

**Fix:** Quote the value in the example YAML:
```yaml
flinkConfiguration:
  taskmanager.numberOfTaskSlots: "2"
```

### Missing service account

The example YAML references a `flink` service account that doesn't exist by default:

```bash
kubectl create serviceaccount flink -n flink-test
kubectl create clusterrolebinding flink-role-binding \
  --clusterrole=edit \
  --serviceaccount=flink-test:flink
```

---

## Troubleshooting

### "kubectl can't connect to cluster"
- Make sure Kubernetes is enabled in Docker Desktop
- Restart Docker Desktop
- Check Docker Desktop shows green "Kubernetes running" in bottom left

### "helm: command not found"
- Close and reopen Git Bash after installing helm
- Verify helm is on your PATH

### Port-forward dies immediately
- The Flink job might still be starting. Wait 30s and try again.
- Check pod status: `kubectl get pods -n flink-test`
