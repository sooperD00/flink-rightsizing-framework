#!/bin/bash
# =============================================================================
# setup_local_cluster.sh
# 
# Sets up a local Flink-on-Kubernetes cluster for testing.
# Designed for Windows 10 + Git Bash + Docker Desktop.
#
# Prerequisites (install these first):
#   1. Docker Desktop for Windows (with Kubernetes enabled)
#   2. kubectl (comes with Docker Desktop, or: choco install kubernetes-cli)
#   3. helm (choco install kubernetes-helm, or: winget install Helm.Helm)
#
# Usage:
#   ./setup_local_cluster.sh          # full setup
#   ./setup_local_cluster.sh check    # just check prerequisites
#   ./setup_local_cluster.sh teardown # remove everything
# =============================================================================

set -e

# Colors (work in Git Bash)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_step() { echo -e "${GREEN}==>${NC} $1"; }
echo_warn() { echo -e "${YELLOW}WARNING:${NC} $1"; }
echo_error() { echo -e "${RED}ERROR:${NC} $1"; }

# =============================================================================
# Prerequisite checks
# =============================================================================

check_prerequisites() {
    echo_step "Checking prerequisites..."
    
    local missing=0
    
    # Docker
    if command -v docker &> /dev/null; then
        if docker info &> /dev/null; then
            echo "  ✓ Docker is running"
        else
            echo_error "Docker is installed but not running. Start Docker Desktop first."
            missing=1
        fi
    else
        echo_error "Docker not found. Install Docker Desktop for Windows."
        echo "  https://www.docker.com/products/docker-desktop/"
        missing=1
    fi
    
    # kubectl
    if command -v kubectl &> /dev/null; then
        if kubectl cluster-info &> /dev/null 2>&1; then
            echo "  ✓ kubectl connected to cluster"
        else
            echo_error "kubectl found but can't connect to cluster."
            echo "  Enable Kubernetes in Docker Desktop: Settings > Kubernetes > Enable"
            missing=1
        fi
    else
        echo_error "kubectl not found. Install via:"
        echo "  choco install kubernetes-cli"
        echo "  OR it comes with Docker Desktop (restart terminal after install)"
        missing=1
    fi
    
    # helm
    if command -v helm &> /dev/null; then
        echo "  ✓ helm $(helm version --short)"
    else
        echo_error "helm not found. Install via:"
        echo "  choco install kubernetes-helm"
        echo "  OR: winget install Helm.Helm"
        missing=1
    fi
    
    if [ $missing -eq 1 ]; then
        echo ""
        echo_error "Missing prerequisites. Install them and re-run."
        exit 1
    fi
    
    echo_step "All prerequisites met!"
}

# =============================================================================
# Install Flink Kubernetes Operator
# =============================================================================

install_flink_operator() {
    echo_step "Installing Flink Kubernetes Operator..."
    
    # Check if already installed
    if helm list -n flink-operator 2>/dev/null | grep -q flink-kubernetes-operator; then
        echo "  Flink operator already installed, skipping."
        return
    fi
    
    # Add helm repo
    helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.10.0/ 2>/dev/null || true
    helm repo update
    
    # Create namespace
    kubectl create namespace flink-operator 2>/dev/null || true
    
    # Install cert-manager (required by Flink operator)
    echo_step "Installing cert-manager (required dependency)..."
    kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.4/cert-manager.yaml
    
    # Wait for cert-manager
    echo "  Waiting for cert-manager to be ready (this takes ~60s)..."
    kubectl wait --for=condition=Available deployment/cert-manager -n cert-manager --timeout=120s
    kubectl wait --for=condition=Available deployment/cert-manager-webhook -n cert-manager --timeout=120s
    
    # Install Flink operator
    helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
        --namespace flink-operator
    
    # Wait for operator
    echo "  Waiting for Flink operator to be ready..."
    kubectl wait --for=condition=Available deployment/flink-kubernetes-operator \
        -n flink-operator --timeout=120s
    
    echo_step "Flink operator installed!"
}

# =============================================================================
# Deploy example Flink job
# =============================================================================

deploy_example_job() {
    echo_step "Deploying example Flink job..."
    
    # Create namespace for our test
    kubectl create namespace flink-test 2>/dev/null || true
    
    # Check if already deployed
    if kubectl get flinkdeployment basic-example -n flink-test &> /dev/null; then
        echo "  Example job already deployed, skipping."
        return
    fi
    
    # Deploy basic example
    kubectl apply -n flink-test -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/main/examples/basic.yaml
    
    # Wait for job to be running
    echo "  Waiting for Flink job to start (this takes ~90s)..."
    sleep 10  # Give it a moment to create resources
    
    # Wait for JobManager pod
    for i in {1..30}; do
        if kubectl get pods -n flink-test -l component=jobmanager -o jsonpath='{.items[0].status.phase}' 2>/dev/null | grep -q Running; then
            echo "  ✓ JobManager is running"
            break
        fi
        echo "  Waiting for JobManager... ($i/30)"
        sleep 5
    done
    
    echo_step "Example Flink job deployed!"
}

# =============================================================================
# Port forward (run in foreground)
# =============================================================================

start_port_forward() {
    echo_step "Starting port-forward to Flink REST API..."
    echo ""
    echo "  Flink UI will be available at: http://localhost:8081"
    echo "  Press Ctrl+C to stop."
    echo ""
    
    # Find the JobManager service
    kubectl port-forward -n flink-test svc/basic-example-rest 8081:8081
}

# =============================================================================
# Teardown
# =============================================================================

teardown() {
    echo_step "Tearing down Flink test cluster..."
    
    # Delete example job
    kubectl delete -n flink-test -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/main/examples/basic.yaml 2>/dev/null || true
    kubectl delete namespace flink-test 2>/dev/null || true
    
    # Uninstall Flink operator
    helm uninstall flink-kubernetes-operator -n flink-operator 2>/dev/null || true
    kubectl delete namespace flink-operator 2>/dev/null || true
    
    # Optionally remove cert-manager (commented out - you might want to keep it)
    # kubectl delete -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.4/cert-manager.yaml
    
    echo_step "Teardown complete!"
}

# =============================================================================
# Main
# =============================================================================

case "${1:-}" in
    check)
        check_prerequisites
        ;;
    teardown)
        teardown
        ;;
    forward|port-forward)
        start_port_forward
        ;;
    *)
        check_prerequisites
        echo ""
        install_flink_operator
        echo ""
        deploy_example_job
        echo ""
        echo_step "Setup complete!"
        echo ""
        echo "Next steps:"
        echo "  1. Start port-forward (in a separate terminal):"
        echo "     ./setup_local_cluster.sh forward"
        echo ""
        echo "  2. Test the connection:"
        echo "     python flink_client.py"
        echo ""
        echo "  3. Run observe:"
        echo "     python 0_observe.py --stdout"
        echo ""
        echo "  4. When done:"
        echo "     ./setup_local_cluster.sh teardown"
        ;;
esac
