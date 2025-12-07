#!/bin/bash
# Script to run batch processing on Kubernetes

# Create a pod with Spark
kubectl run spark-batch --rm -it --image=bitnami/spark:3.5 --restart=Never -n bigdata-pipeline -- bash << 'EOF'

# Install dependencies
pip install hdfs3 pyhdfs

# Copy batch script
# (Run this from your local machine first: kubectl cp batch/submit.py bigdata-pipeline/spark-batch:/tmp/submit.py)

# Run batch job
python /tmp/submit.py

EOF

