@echo off
TITLE Resume Big Data Cluster
echo ========================================================
echo   DANH THUC CLUSTER SAU KHI KHOI DONG LAI MAY
echo ========================================================

:: 1. Khoi dong Container cua Kind
echo [1/2] Dang bat lai Docker Container (kind-control-plane)...
docker start bigdata-pipeline-control-plane

:: Neu ban khong dat ten cluster rieng thi ten mac dinh la: kind-control-plane
:: docker start kind-control-plane

echo.
echo    -> Dang cho Kubernetes khoi dong services (30s)...
timeout /t 30 /nobreak >nul

:: 2. Thiet lap lai ngu canh (Context)
echo [2/2] Kiem tra ket noi...
kubectl cluster-info --context kind-bigdata-pipeline
kubectl config set-context --current --namespace=bigdata-pipeline

echo.
echo ========================================================
echo   HE THONG DA SAN SANG!
echo ========================================================
kubectl get pods
pause