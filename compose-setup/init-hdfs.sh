#!/bin/bash

# Lấy tên pod namenode
NAMENODE_POD=$(kubectl get pods -n bigdata-pipeline -l app=namenode -o jsonpath='{.items[0].metadata.name}')

echo "Using namenode pod: $NAMENODE_POD"

# Xóa thư mục cũ nếu có (tùy chọn)
echo "Cleaning old directories..."
kubectl exec -n bigdata-pipeline $NAMENODE_POD -- hdfs dfs -rm -r /user 2>/dev/null || true

# Tạo các thư mục cần thiết
echo "Creating /user directory..."
kubectl exec -n bigdata-pipeline $NAMENODE_POD -- hdfs dfs -mkdir -p /user

echo "Creating /user/pdt directory..."
kubectl exec -n bigdata-pipeline $NAMENODE_POD -- hdfs dfs -mkdir -p /user/pdt

echo "Creating /user/pdt/raw_rows directory..."
kubectl exec -n bigdata-pipeline $NAMENODE_POD -- hdfs dfs -mkdir -p /user/pdt/raw_rows

echo "Creating /user/pdt/processed directory..."
kubectl exec -n bigdata-pipeline $NAMENODE_POD -- hdfs dfs -mkdir -p /user/pdt/processed

# Kiểm tra kết quả
echo "Listing HDFS directories:"
kubectl exec -n bigdata-pipeline $NAMENODE_POD -- hdfs dfs -ls /
kubectl exec -n bigdata-pipeline $NAMENODE_POD -- hdfs dfs -ls /user
kubectl exec -n bigdata-pipeline $NAMENODE_POD -- hdfs dfs -ls /user/pdt

echo "HDFS initialization completed!"