@echo off
TITLE Big Data Pipeline Installer
echo ========================================================
echo   KHOI TAO MOI TRUONG BIG DATA (KUBERNETES - KIND)
echo ========================================================

:: 1. Tao Namespace
echo [1/3] Tao Namespace 'bigdata-pipeline'...
kubectl create namespace bigdata-pipeline
kubectl config set-context --current --namespace=bigdata-pipeline
echo.

:: 2. Deploy Ha tang (Infrastructure Layer)
echo [2/3] Dang deploy ha tang...

echo    -> Deploy Zookeeper...
kubectl apply -f D:\CODING_AREA\BD\Big2025\TrafficCrash\compose-setup\zookeeper.yaml
echo    -> Dang cho Zookeeper khoi dong (15s)...
timeout /t 15 /nobreak >nul

echo    -> Deploy HDFS Namenode & Kafka...
kubectl apply -f D:\CODING_AREA\BD\Big2025\TrafficCrash\compose-setup\hdfs-namenode.yaml
kubectl apply -f D:\CODING_AREA\BD\Big2025\TrafficCrash\compose-setup\kafka.yaml
echo    -> Dang cho Namenode format o cung (20s)...
timeout /t 20 /nobreak >nul

echo    -> Deploy HDFS Datanode & Cassandra...
kubectl apply -f D:\CODING_AREA\BD\Big2025\TrafficCrash\compose-setup\hdfs-datanode.yaml
kubectl apply -f D:\CODING_AREA\BD\Big2025\TrafficCrash\compose-setup\cassandra.yaml

echo.

:: 3. Deploy Ung dung (Compute Layer)
echo [3/3] Dang deploy Spark & App...
:: kubectl apply -f D:\CODING_AREA\BD\Big2025\TrafficCrash\compose-setup\spark.yaml
:: kubectl apply -f D:\CODING_AREA\BD\Big2025\TrafficCrash\compose-setup\trino.yaml
:: kubectl apply -f D:\CODING_AREA\BD\Big2025\TrafficCrash\compose-setup\superset.yaml

echo ========================================================
echo   CAI DAT HOAN TAT! 
echo   Hay kiem tra trang thai bang lenh: kubectl get pods
echo ========================================================
pause