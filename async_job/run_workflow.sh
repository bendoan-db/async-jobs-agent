#!/bin/bash
#
# Deploy and run the async_job workflow
#
# Usage:
#   ./run_workflow.sh [OPTIONS]
#
# Options:
#   --profile PROFILE      Databricks CLI profile (default: DEFAULT)
#   --target TARGET        Deployment target: dev or prod (default: dev)
#   --skip-validation      Skip bundle validation
#   --skip-deployment      Skip deployment, just run existing job
#   --job-id JOB_ID        Run existing job by ID (requires --skip-deployment)
#   --var KEY=VALUE        Override variable (can be repeated)
#   -h, --help             Show this help message

set -e

# Default values
PROFILE="DEFAULT"
TARGET="dev"
SKIP_VALIDATION=false
SKIP_DEPLOYMENT=false
JOB_ID=""
VARS=()

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --target)
            TARGET="$2"
            shift 2
            ;;
        --skip-validation)
            SKIP_VALIDATION=true
            shift
            ;;
        --skip-deployment)
            SKIP_DEPLOYMENT=true
            shift
            ;;
        --job-id)
            JOB_ID="$2"
            shift 2
            ;;
        --var)
            VARS+=("--var" "$2")
            shift 2
            ;;
        -h|--help)
            head -20 "$0" | tail -n +2 | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Build common args
COMMON_ARGS="-t $TARGET"
if [ "$PROFILE" != "DEFAULT" ]; then
    COMMON_ARGS="$COMMON_ARGS --profile $PROFILE"
fi

# Add variable overrides
for var in "${VARS[@]}"; do
    COMMON_ARGS="$COMMON_ARGS $var"
done

echo "=============================================="
echo "Async Job Workflow"
echo "=============================================="
echo "Profile: $PROFILE"
echo "Target: $TARGET"
echo "=============================================="

# Change to script directory
cd "$(dirname "$0")"

# Validate bundle
if [ "$SKIP_VALIDATION" = false ]; then
    echo ""
    echo "[1/3] Validating bundle..."
    databricks bundle validate $COMMON_ARGS
    echo "✅ Validation passed"
else
    echo ""
    echo "[1/3] Skipping validation"
fi

# Deploy bundle
if [ "$SKIP_DEPLOYMENT" = false ]; then
    echo ""
    echo "[2/3] Deploying bundle..."
    databricks bundle deploy $COMMON_ARGS
    echo "✅ Deployment complete"
else
    echo ""
    echo "[2/3] Skipping deployment"
fi

# Run the job
echo ""
echo "[3/3] Running job..."

if [ -n "$JOB_ID" ]; then
    # Run existing job by ID
    RUN_OUTPUT=$(databricks jobs run-now --job-id "$JOB_ID" --profile "$PROFILE")
else
    # Run deployed job
    RUN_OUTPUT=$(databricks bundle run async_job $COMMON_ARGS 2>&1)
fi

echo "$RUN_OUTPUT"

# Extract run ID if available
RUN_ID=$(echo "$RUN_OUTPUT" | grep -o 'run_id.*[0-9]\+' | grep -o '[0-9]\+' | head -1 || true)

if [ -n "$RUN_ID" ]; then
    echo ""
    echo "=============================================="
    echo "✅ Job started with run ID: $RUN_ID"
    echo "=============================================="
fi

echo ""
echo "Done!"
