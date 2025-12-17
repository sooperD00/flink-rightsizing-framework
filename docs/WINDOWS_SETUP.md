# Windows 10 Setup Guide

## Prerequisites

You need three things before running `setup_local_cluster.sh`:

### 1. Docker Desktop for Windows

Download: https://www.docker.com/products/docker-desktop/
- Intel i7 is AMD64 architechture
- Windows10 is sufficient

What is Docker Desktop?
- a way to run Linux on your Windows machine so you can run containers
- Do not select "use Windows containers" - you want Linux

What it gives you:
- **A lightweight Linux VM running invisibly in the background**
- The docker command to build/run containers
- A built-in Kubernetes cluster (what you'll enable next)
- A GUI to see what's running
- and more (shiny... distraction...)
| | Cygwin | WSL/Docker |
|---|--------|------------|
| What it is | Compatibility layer + ported GNU tools | Actual Linux kernel running |
| Era | 2000s pain | Modern solution |
| "Feels like" | Linux cosplay | Real Linux |
| Paths | `/cygdrive/c/Users/...` nightmares | Clean integration |
>Wow, Welcome to 2025 🎉

Why you need it:
- **Flink runs in containers**
- **Kubernetes orchestrates containers**
- Your 0_observe.py script needs a Flink cluster to talk to
- This is the cheapest way to get one (free, local, no cloud bill)

What it is NOT:
- Production infrastructure
- GKE/cloud — that comes in Week 4
- Complicated — it's basically "one click to get Linux containers on Windows"

After install:
1. Open Docker Desktop
	- you may need to update Windows Subsystem for Linux (WSL)
	- win > powershell (run as administrator) > `wsl --update` > restart/retry
2. Go to **Settings** (gear icon)
3. Go to **Kubernetes**
4. Check **Enable Kubernetes**
	- [x] Choose Kubeadm (default, single-node cluster)
	- [ ] "show system containers" (no)
5. Click **Apply & Restart**
6. Wait for the green "Kubernetes running" indicator (bottom left)

### 2. kubectl

**This is the CLI for Kubernetes**

Usually comes with Docker Desktop. Test it:
```bash
kubectl version --client
```
Output 12/16/2025:
```bash
$ kubectl version --client
Client Version: v1.34.1
Kustomize Version: v5.7.1
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

**This is the package manager for Kubernetes**

Install via **winget** (easiest, no extra install needed):
```powershell
winget install Helm.Helm
```

Or via Chocolatey:
```powershell
choco install kubernetes-helm
```

Or download: https://helm.sh/docs/intro/install/
Or: https://github.com/helm/helm/releases/latest
- Helm is a CNCF graduated project (same foundation as Kubernetes), so while there might be one primary maintainer doing releases, it has organizational backing.


Just do the direct download then:
- Go to: https://github.com/helm/helm/releases/latest
- Find `helm-v3.16.x-windows-amd64.zip` (or whatever the latest version number is)
- Download it
- Extract to C:\tools\helm
- Add to PATH:
	- Windows key → "environment variables" → click "Edit the system environment variables"
	- Click "Environment Variables" button
	- Under "User variables", find Path, click Edit
	- Click New → add C:\tools\helm
	- OK → OK → OK
- Close and reopen PowerShell
- Test: helm version

Output 12/16/2025
```powershell
PS C:\WINDOWS\system32> helm version
version.BuildInfo{Version:"v4.0.4", GitCommit:"8650e1dad9e6ae38b41f60b712af9218a0d8cc11", GitTreeState:"clean", GoVersion:"go1.25.5", KubeClientVersion:"v1.34"}
PS C:\WINDOWS\system32>
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

# 4. Back in first window, test it, from the scripts/ directory
cd ../scripts
python flink_client.py
python 0_observe.py --stdout

# 5. When done
./setup_local_cluster.sh teardown
```

Output 12/16/2025
```bash
$ ./setup_local_cluster.sh
==> Checking prerequisites...
  ✓ Docker is running
  ✓ kubectl connected to cluster
  ✓ helm v4.0.4+g8650e1d
==> All prerequisites met!

$ ./setup_local_cluster.sh
==> Checking prerequisites...
  ✓ Docker is running
  ✓ kubectl connected to cluster
  ✓ helm v4.0.4+g8650e1d
==> All prerequisites met!

==> Installing Flink Kubernetes Operator...
"flink-operator-repo" has been added to your repositories
Hang tight while we grab the latest from your chart repositories...
...Successfully got an update from the "flink-operator-repo" chart repository
Update Complete. ⎈Happy Helming!⎈
namespace/flink-operator created
==> Installing cert-manager (required dependency)...
namespace/cert-manager created

...

  Waiting for cert-manager to be ready (this takes ~60s)...
deployment.apps/cert-manager condition met
deployment.apps/cert-manager-webhook condition met
NAME: flink-kubernetes-operator
LAST DEPLOYED: Tue Dec 16 21:57:46 2025
NAMESPACE: flink-operator
STATUS: deployed
REVISION: 1
DESCRIPTION: Install complete
TEST SUITE: None
  Waiting for Flink operator to be ready...
deployment.apps/flink-kubernetes-operator condition met
==> Flink operator installed!

==> Deploying example Flink job...
namespace/flink-test created
The FlinkDeployment "basic-example" is invalid: spec.flinkConfiguration.taskmanager.numberOfTaskSlots: Invalid value: "integer": spec.flinkConfigurati
on.taskmanager.numberOfTaskSlots in body must be of type string: "integer"
```

Version mismatch between the operator and the example YAML. Quick fix:
```bash
# Download the example
curl -o basic-example.yaml https://raw.githubusercontent.com/apache/flink-kubernetes-operator/main/examples/basic.yaml

# Look at it
cat basic-example.yaml
```

The Flink operator wants all config values as strings. Their own example is wrong. I guess open source is still open source, even in 2025. 🎉 Change this line to (add quotes):
```yaml
flinkConfiguration:
  taskmanager.numberOfTaskSlots: "2"
```

Apply:
```bash
kubectl apply -n flink-test -f basic-example.yaml
```

Output 12/16/2025
```bash
$ ./setup_local_cluster.sh
==> Checking prerequisites...
  ✓ Docker is running
  ✓ kubectl connected to cluster
  ✓ helm v4.0.4+g8650e1d
==> All prerequisites met!

==> Installing Flink Kubernetes Operator...
  Flink operator already installed, skipping.

==> Deploying example Flink job...
  Example job already deployed, skipping.

==> Setup complete!

Next steps:
  1. Start port-forward (in a separate terminal):
     ./setup_local_cluster.sh forward

  2. Test the connection:
     python flink_client.py

  3. Run observe:
     python 0_observe.py --stdout

  4. When done:
     ./setup_local_cluster.sh teardown
```

```bash
$ ./setup_local_cluster.sh forward
==> Starting port-forward to Flink REST API...

  Flink UI will be available at: http://localhost:8081
  Press Ctrl+C to stop.
```

```bash
$ python flink_client.py
Could not connect to http://localhost:8081
```

```bash
#The YAML references a service account that doesn't exist. Create it:
kubectl create serviceaccount flink -n flink-test

# Then give it permissions:
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
- Or add helm to your PATH manually

### Port-forward dies immediately
- The Flink job might still be starting. Wait 30s and try again.
- Check pod status: `kubectl get pods -n flink-test`

### Everything is slow
- Docker Desktop on Windows needs decent resources
- Allocate **at least 4 CPU / 8GB RAM** in Docker Desktop settings
	- Processor	Intel(R) Core(TM) i7-6500U CPU @ 2.50GHz, 2592 Mhz, 2 Core(s), 4 Logical Processor(s), Installed Physical Memory (RAM)	16.0 GB
		- You have plenty of RAM. Set Docker Desktop to:
			- CPU: 2
			- Memory: 6 GB
		- Your CPU is the bottleneck (2 cores), but it's enough for a toy cluster. Just expect things to take a minute or two instead of seconds.
	- Docker Desktop → Settings → Resources
		- You are using the WSL 2 backend, so resource limits are managed by Windows
		- WSL2 manages resources dynamically — you're probably fine as-is with 16GB RAM. It'll take what it needs.

---

## What Gets Installed

| Component | Namespace | Purpose |
|-----------|-----------|---------|
| cert-manager | cert-manager | Required by Flink operator |
| flink-kubernetes-operator | flink-operator | Manages Flink deployments |
| basic-example | flink-test | Sample Flink job for testing |

All contained in your local Docker Desktop Kubernetes. Nothing touches cloud/GCP.
