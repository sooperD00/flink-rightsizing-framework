# Windows 10 Setup Guide

## Prerequisites

You need three things before running `bin/local_cluster.sh`:

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

## Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate    # Git Bash
pip install -r requirements.txt
```

Activate the venv in every new terminal before running Python scripts.

---

## Quick Start

After prerequisites are installed:

```bash
# In Git Bash, from the project root

# 1. Check everything is ready
bash bin/validate_env.sh

# 2. Install Flink operator + example job (~3-5 min)
bash bin/local_cluster.sh

# 3. In a SEPARATE Git Bash window, start port-forward
bash bin/local_cluster.sh forward

# 4. Back in first window (activate venv first)
source .venv/bin/activate
python3 scripts/flink_client.py
bash bin/run_observe.sh
bash bin/run_classify.sh

# 5. When done
bash bin/local_cluster.sh teardown
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

### Helm repo URL changes with operator versions

The Flink operator helm chart URL includes a version number and older URLs get moved to `archive.apache.org`. If `local_cluster.sh` fails on `helm repo add`, check for the latest version at https://flink.apache.org/downloads/ and update the URL in the script. As of February 2026, the working URL is:

```
https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.13.0/
```

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

### Missing service account and RBAC

The example YAML references a `flink` service account that doesn't exist by default in the test namespace. The `local_cluster.sh` script handles this automatically, but if you're setting up manually:

```bash
kubectl create serviceaccount flink -n flink-test

kubectl create role flink-role -n flink-test \
  --verb=get,list,watch,create,delete,patch,update \
  --resource=pods,services,configmaps,deployments,replicasets

kubectl create rolebinding flink-role-binding -n flink-test \
  --role=flink-role \
  --serviceaccount=flink-test:flink
```

(Uses a namespace-scoped role rather than a cluster-wide binding — principle of least privilege.)

### Backpressure endpoint returns 500 on fresh cluster

The Flink backpressure monitoring endpoint needs ~30 seconds to warm up after a job starts. The observe script retries automatically, but if you're calling the REST API directly, expect a 500 for the first few attempts.

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
