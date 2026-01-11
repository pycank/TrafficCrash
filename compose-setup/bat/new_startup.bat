@echo off
TITLE Kubernetes Deployer (Kubectl Only)
echo ========================================================
echo   TRIEN KHAI HE THONG BIG DATA (PURE KUBECTL)
echo ========================================================

:: 1. Tao Namespace (Neu chua co)
echo [1/5] Kiem tra Namespace...
kubectl create namespace bigdata-pipeline --dry-run=client -o yaml | kubectl apply -f -
kubectl config set-context --current --namespace=bigdata-pipeline
echo.

:: 2. Deploy Zookeeper (Tang ha tang 1)
echo [2/5] Deploying Zookeeper...
kubectl apply -f k8s/01-zookeeper.yaml

echo       ...Dang cho Zookeeper san sang...
:: Lenh nay se dung doi den khi Zookeeper bao tin hieu Ready
kubectl wait --for=condition=ready pod -l app=zookeeper --timeout=120s -n bigdata-pipeline
echo.

:: 3. Deploy Kafka & Namenode (Tang ha tang 2)
echo [3/5] Deploying Kafka & HDFS Namenode...
kubectl apply -f k8s/03-kafka.yaml
kubectl apply -f k8s/02-namenode.yaml

echo       ...Dang cho Kafka san sang...
kubectl wait --for=condition=ready pod -l app=kafka --timeout=300s -n bigdata-pipeline

echo       ...Dang cho Namenode san sang...
kubectl wait --for=condition=ready pod -l app=hdfs-namenode --timeout=300s -n bigdata-pipeline
echo.

:: 4. Deploy Datanode & Cassandra (Tang ha tang 3)
echo [4/5] Deploying HDFS Datanode & Cassandra...
kubectl apply -f k8s/03-datanode.yaml
kubectl apply -f k8s/04-cassandra.yaml

echo       ...Dang cho Datanode san sang...
kubectl wait --for=condition=ready pod -l app=hdfs-datanode --timeout=300s -n bigdata-pipeline

echo       ...Dang cho Cassandra san sang...
kubectl wait --for=condition=ready pod -l app=cassandra --timeout=300s -n bigdata-pipeline
echo.

:: 5. Deploy App (Spark, Trino...) - Uncomment neu co file
:: echo [5/5] Deploying Spark Cluster...
:: kubectl apply -f k8s/05-spark.yaml

echo ========================================================
echo   TRIEN KHAI HOAN TAT! HE THONG DA SAN SANG.
echo ========================================================
kubectl get pods
pause