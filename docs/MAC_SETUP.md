# macOS Setup Guide

Tested on macOS Sequoia 15.3.1 (Apple Silicon / arm64, Mac Mini).

## Prerequisites

### 1. Homebrew

The package manager for macOS. Install first — everything else flows from here.

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

After install, make sure brew is on your PATH:

```bash
echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### 2. Python

macOS ships with an older system Python. Install a current version via brew:

```bash
brew install python
```

Verify it's the brew version (not the system one):

```bash
which python3
# Should show: /opt/homebrew/bin/python3
```

### 3. Docker Desktop for Mac

Download: https://www.docker.com/products/docker-desktop/ — select **Mac with Apple Chip** (for M-series).

Open the `.dmg`, drag Docker into Applications, launch it, and grant the permissions it asks for.

**Important settings** (Docker Desktop → Settings):
- **Kubernetes:** Enable Kubernetes (kubeadm, single-node). Apply & Restart.
- **Resources:** At least 4 CPU / 8 GB RAM (defaults are usually fine with 16GB system RAM).
- **Resource Saver: Turn this OFF.** It pauses Docker when idle, which kills Flink pods.

Wait for both the Docker and Kubernetes indicators to show green before proceeding.

### 4. Helm

```bash
brew install helm
```

### 5. kubectl

Comes with Docker Desktop — verify with:

```bash
kubectl version --client
```

### 6. Git

Usually pre-installed. Running `git --version` will either confirm it or prompt you to install Xcode Command Line Tools automatically.

---

## Python Environment

Modern Python (3.12+) enforces virtual environments. Create one in the project root:

```bash
cd ~/repos/flink-rightsizing-framework
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

You'll need to `source .venv/bin/activate` in each new terminal before running Python scripts. VS Code auto-detects the venv and activates it in its integrated terminal.

---

## Quick Start

```bash
# 1. Check everything is ready
bash bin/validate_env.sh

# 2. Install Flink operator + example job (~3-5 min first time)
bash bin/local_cluster.sh

# 3. In a SEPARATE terminal, start port-forward
bash bin/local_cluster.sh forward

# 4. In a THIRD terminal (activate venv first)
source .venv/bin/activate
python3 scripts/flink_client.py
bash bin/run_observe.sh
bash bin/run_classify.sh

# 5. When done
bash bin/local_cluster.sh teardown
```

iTerm2 tip: **Cmd+D** splits the current pane vertically, so you can run all three terminals in one window.

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

### Backpressure endpoint returns 500 on fresh cluster

The Flink backpressure monitoring endpoint needs ~30 seconds to warm up after a job starts. The observe script retries automatically, but if you're calling the REST API directly, expect a 500 for the first few attempts.

### Docker image platform warnings

On Apple Silicon, some Docker images may show warnings about `linux/amd64` platform. Most Flink images run fine via emulation. If a container crashes on startup, try adding `--platform linux/amd64` explicitly or check if an `arm64` image is available.

---

## Optional but Recommended

**iTerm2** — a better terminal than the built-in Terminal.app. Download from https://iterm2.com/, unzip, drag to Applications.

**Colorized terminal output:**

```bash
echo "alias ls='ls -G'" >> ~/.zshrc
source ~/.zshrc
```

Or for even nicer output: `brew install lsd`

**Finder tips for navigating project files:**
- **Cmd+Shift+H** — jump to home directory
- **Cmd+Shift+.** — toggle hidden files (`.venv`, `.gitignore`, etc.)
- **View → Show Path Bar** — always see the full path at bottom of Finder

---

## Troubleshooting

### "kubectl can't connect to cluster"
- Make sure Kubernetes is enabled in Docker Desktop (Settings → Kubernetes)
- Restart Docker Desktop
- Check both Docker and Kubernetes show green indicators

### "externally-managed-environment" error from pip
- You're not in the venv. Run `source .venv/bin/activate` first.
- Your shell might have picked up the system Python. Check `which python3` — it should show `.venv/bin/python3` when activated.

### Port-forward dies or times out
- The Flink job might still be starting. Wait 30s and try again.
- Check pod status: `kubectl get pods -n flink-test`
- If pods show `CrashLoopBackOff`, check logs: `kubectl logs -n flink-test -l component=jobmanager --previous`

### brew Python not found after install
- Add brew to PATH: `echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.zshrc && source ~/.zshrc`
- Verify: `which python3` should show `/opt/homebrew/bin/python3`
