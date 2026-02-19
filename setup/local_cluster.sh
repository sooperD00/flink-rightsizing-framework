# setup/local_cluster.sh
#!/bin/bash
# =============================================================================
# Local Flink-on-K8s Development Environment
# =============================================================================
# Prerequisites: Docker Desktop with Kubernetes enabled, helm, kubectl
#
# Usage:
#   ./local_cluster.sh          # Start cluster + deploy example job
#   ./local_cluster.sh forward  # Port-forward to access REST API
#   ./local_cluster.sh status   # Check what's running
#   ./local_cluster.sh stop     # Tear it all down
# =============================================================================

set -e  # Exit on error

NAMESPACE="flink"
RELEASE_NAME="flink-operator"

case "${1:-start}" in
  start)
    echo "=== Starting local Flink cluster ==="
    
    # Create namespace
    kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
    
    # Install Flink Kubernetes Operator
    helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.10.0/ 2>/dev/null || true
    helm repo update
    helm upgrade --install $RELEASE_NAME flink-operator/flink-kubernetes-operator \
      --namespace $NAMESPACE \
      --wait
    
    # Deploy example Flink job
    kubectl apply -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/main/examples/basic.yaml \
      -n $NAMESPACE
    
    echo ""
    echo "=== Waiting for pods ==="
    kubectl wait --for=condition=ready pod -l app=basic-example -n $NAMESPACE --timeout=120s
    
    echo ""
    echo "=== Cluster ready ==="
    echo "Run: ./local_cluster.sh forward"
    echo "Then: python scripts/flink_client.py"
    ;;
    
  forward)
    echo "=== Port-forwarding to Flink REST API ==="
    echo "Access at: http://localhost:8081"
    echo "Press Ctrl+C to stop"
    kubectl port-forward svc/basic-example-rest 8081:8081 -n $NAMESPACE
    ;;
    
  status)
    echo "=== Flink namespace status ==="
    kubectl get pods -n $NAMESPACE
    echo ""
    kubectl get svc -n $NAMESPACE
    ;;
    
  stop)
    echo "=== Tearing down local cluster ==="
    kubectl delete -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/main/examples/basic.yaml \
      -n $NAMESPACE --ignore-not-found
    helm uninstall $RELEASE_NAME -n $NAMESPACE --ignore-not-found || true
    kubectl delete namespace $NAMESPACE --ignore-not-found
    echo "=== Done ==="
    ;;
    
  *)
    echo "Usage: $0 {start|forward|status|stop}"
    exit 1
    ;;
esac