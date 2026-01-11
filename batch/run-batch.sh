#!/bin/bash
# Script to run batch processing on Kubernetes

set -e

NAMESPACE="bigdata-pipeline"
POD_NAME="spark-batch-$(date +%s)"
SCRIPT_PATH="batch/submit.py"

echo "========================================="
echo "Spark Batch Processing Script"
echo "========================================="

# Step 1: Create a pod with Spark
echo "[1/4] Creating Spark pod: $POD_NAME"
kubectl run $POD_NAME \
    --image=bitnami/spark:3.5 \
    --restart=Never \
    -n $NAMESPACE \
    --command -- sleep 3600

# Wait for pod to be ready
echo "[2/4] Waiting for pod to be ready..."
kubectl wait --for=condition=Ready pod/$POD_NAME -n $NAMESPACE --timeout=60s

# Step 2: Install Python dependencies
echo "[3/4] Installing Python dependencies..."
kubectl exec -n $NAMESPACE $POD_NAME -- pip install hdfs3 pyhdfs cassandra-driver --quiet

# Step 3: Copy batch script to pod
echo "[4/4] Copying batch script to pod..."
kubectl cp $SCRIPT_PATH $NAMESPACE/$POD_NAME:/tmp/submit.py

# Step 4: Run Spark job using spark-submit
echo "========================================="
echo "Running Spark batch job..."
echo "========================================="
kubectl exec -n $NAMESPACE $POD_NAME -- \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.driver.memory=1g \
    --conf spark.executor.memory=1g \
    /tmp/submit.py

# Get job result
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "========================================="
    echo "Batch job completed successfully!"
    echo "========================================="
else
    echo "========================================="
    echo "Batch job failed with exit code: $EXIT_CODE"
    echo "========================================="
fi

# Step 5: Cleanup - delete pod
echo "Cleaning up pod..."
kubectl delete pod $POD_NAME -n $NAMESPACE --ignore-not-found=true

exit $EXIT_CODE