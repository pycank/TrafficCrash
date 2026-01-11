@echo off
TITLE Initialize HDFS Directories
echo ========================================================
echo   KHOI TAO CAU TRUC THU MUC HDFS
echo ========================================================

:: 1. Xoa thu muc cu (Neu muon reset sach se)
echo [1/4] Dang xoa thu muc /user cu...
kubectl exec hdfs-namenode-0 -n bigdata-pipeline -- hdfs dfs -rm -r /user
echo.

:: 2. Tao thu muc moi
echo [2/4] Dang tao thu muc /user/pdt/raw_rows...
kubectl exec hdfs-namenode-0 -n bigdata-pipeline -- hdfs dfs -mkdir -p /user/pdt/raw_rows

echo [3/4] Dang tao thu muc /user/pdt/processed...
kubectl exec hdfs-namenode-0 -n bigdata-pipeline -- hdfs dfs -mkdir -p /user/pdt/processed
echo.

:: 3. Cap quyen ghi (De Spark/Kafka ghi duoc vao)
echo [4/4] Dang cap quyen (chmod 777)...
kubectl exec hdfs-namenode-0 -n bigdata-pipeline -- hdfs dfs -chmod -R 777 /user
echo.

echo ========================================================
echo   KET QUA HIEN TAI:
echo ========================================================
kubectl exec hdfs-namenode-0 -n bigdata-pipeline -- hdfs dfs -ls -R /user
pause