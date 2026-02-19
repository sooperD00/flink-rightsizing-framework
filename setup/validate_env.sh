# setup/validate_env.sh
#!/bin/bash
# =============================================================================
# Validate Development Environment
# =============================================================================
# Checks that all prerequisites are installed and configured.
# Run this first if something isn't working.
# =============================================================================

echo "=== Flink Rightsizing Framework: Environment Check ==="
echo ""

ERRORS=0

# Check Python
if command -v python3 &> /dev/null; then
  PYTHON_VERSION=$(python3 --version 2>&1)
  echo "✓ Python:     $PYTHON_VERSION"
else
  echo "✗ Python:     NOT FOUND (need Python 3.8+)"
  ERRORS=$((ERRORS + 1))
fi

# Check pip packages
if python3 -c "import requests" 2>/dev/null; then
  echo "✓ requests:   installed"
else
  echo "✗ requests:   NOT FOUND (run: pip install -r requirements.txt)"
  ERRORS=$((ERRORS + 1))
fi

# Check kubectl
if command -v kubectl &> /dev/null; then
  KUBECTL_VERSION=$(kubectl version --client --short 2>/dev/null || kubectl version --client 2>/dev/null | head -1)
  echo "✓ kubectl:    $KUBECTL_VERSION"
else
  echo "✗ kubectl:    NOT FOUND"
  ERRORS=$((ERRORS + 1))
fi

# Check helm
if command -v helm &> /dev/null; then
  HELM_VERSION=$(helm version --short 2>/dev/null)
  echo "✓ helm:       $HELM_VERSION"
else
  echo "✗ helm:       NOT FOUND"
  ERRORS=$((ERRORS + 1))
fi

# Check Docker
if command -v docker &> /dev/null; then
  if docker info &> /dev/null; then
    echo "✓ docker:     running"
  else
    echo "✗ docker:     installed but not running"
    ERRORS=$((ERRORS + 1))
  fi
else
  echo "✗ docker:     NOT FOUND"
  ERRORS=$((ERRORS + 1))
fi

# Check Kubernetes context
if kubectl cluster-info &> /dev/null 2>&1; then
  CONTEXT=$(kubectl config current-context 2>/dev/null)
  echo "✓ k8s:        connected ($CONTEXT)"
else
  echo "✗ k8s:        not connected (start Docker Desktop with Kubernetes enabled)"
  ERRORS=$((ERRORS + 1))
fi

echo ""
if [[ $ERRORS -eq 0 ]]; then
  echo "=== All checks passed ==="
  echo "Run: ./setup/local_cluster.sh"
else
  echo "=== $ERRORS issue(s) found ==="
  exit 1
fi