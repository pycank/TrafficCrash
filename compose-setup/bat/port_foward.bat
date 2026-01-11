@echo off
TITLE Connect to UI
echo ========================================================
echo   DANG MO KET NOI DEN UI (PORT FORWARDING)
echo   Giu cua so nay luon mo de duy tri ket noi
echo ========================================================

echo 1. HDFS UI: http://localhost:9870
start /b kubectl port-forward svc/hdfs-namenode 9870:9870 -n bigdata-pipeline

echo 2. Cassandra DB: localhost:9042
start /b kubectl port-forward svc/cassandra 9042:9042 -n bigdata-pipeline

:: echo 3. Spark Master UI: http://localhost:8080
:: start /b kubectl port-forward svc/spark-master 8080:8080 -n bigdata-pipeline

echo.
echo Dang chay ngam... (Nhan Ctrl+C de dung tat ca)
pause