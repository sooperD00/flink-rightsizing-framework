# Windows 10 Setup Guide

## Prerequisites

You need three things before running `setup_local_cluster.sh`:

### 1. Docker Desktop for Windows

Download: https://www.docker.com/products/docker-desktop/

After install:
1. Open Docker Desktop
2. Go to **Settings** (gear icon)
3. Go to **Kubernetes**
4. Check **Enable Kubernetes**
5. Click **Apply & Restart**
6. Wait for the green "Kubernetes running" indicator (bottom left)

### 2. kubectl

Usually comes with Docker Desktop. Test it:
```bash
kubectl version --client
```

If not found, install via **winget** (built into Windows 10/11):
```powershell
winget install Kubernetes.kubectl
```

Or via Chocolatey (if you have it):
```powershell
choco install kubernetes-cli
```

Or download directly: https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/

### 3. Helm

Install via **winget** (easiest, no extra install needed):
```powershell
winget install Helm.Helm
```

Or via Chocolatey:
```powershell
choco install kubernetes-helm
```

Or download: https://helm.sh/docs/intro/install/

---

## Quick Start

After prerequisites are installed:

```bash
# In Git Bash, from the scripts/ directory

# 1. Check everything is ready
./setup_local_cluster.sh check

# 2. Install Flink operator + example job (~3-5 min)
./setup_local_cluster.sh

# 3. In a SEPARATE Git Bash window, start port-forward
./setup_local_cluster.sh forward

# 4. Back in first window, test it
python flink_client.py
python 0_observe.py --stdout

# 5. When done
./setup_local_cluster.sh teardown
```

---

## Troubleshooting

### "kubectl can't connect to cluster"
- Make sure Kubernetes is enabled in Docker Desktop
- Restart Docker Desktop
- Check Docker Desktop shows green "Kubernetes running" in bottom left

### "helm: command not found"
- Close and reopen Git Bash after installing helm
- Or add helm to your PATH manually

### Port-forward dies immediately
- The Flink job might still be starting. Wait 30s and try again.
- Check pod status: `kubectl get pods -n flink-test`

### Everything is slow
- Docker Desktop on Windows needs decent resources
- Allocate at least 4 CPU / 8GB RAM in Docker Desktop settings

---

## What Gets Installed

| Component | Namespace | Purpose |
|-----------|-----------|---------|
| cert-manager | cert-manager | Required by Flink operator |
| flink-kubernetes-operator | flink-operator | Manages Flink deployments |
| basic-example | flink-test | Sample Flink job for testing |

All contained in your local Docker Desktop Kubernetes. Nothing touches cloud/GCP.
