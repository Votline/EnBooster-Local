#!/usr/bin/env bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

cleanup() {
    echo -e "\n${YELLOW}==> stopping port-forwarding...${NC}"
    kill $(jobs -p) 2>/dev/null || true
    exit 0
}
trap cleanup SIGINT SIGTERM

echo -e "${GREEN}==> 1. minikube status...${NC}"
if ! minikube status > /dev/null 2>&1; then
    echo -e "${YELLOW}minikube starting...${NC}"
    minikube start
else
    echo -e "${GREEN}minikube started.${NC}"
fi

echo -e "${GREEN}==> 2. changed docker-cli to minikube...${NC}"
eval $(minikube docker-env)

echo -e "${GREEN}==> 3. building containers locally (no cache)...${NC}"
docker build --no-cache -t frontend:latest ./frontend
docker build --no-cache -t gateway:latest -f ./gateway/Dockerfile .
docker build --no-cache -t users-service:latest -f ./users-service/Dockerfile .
docker build --no-cache -t learn-service:latest -f ./learn-service/Dockerfile .
docker build --no-cache -t ai-service:latest -f ./ai-service/Dockerfile .

echo -e "${GREEN}==> 4. loading images into minikube...${NC}"
minikube image load frontend:latest
minikube image load gateway:latest
minikube image load users-service:latest
minikube image load learn-service:latest
minikube image load ai-service:latest

echo -e "${GREEN}==> 5. applying kubernetes-manifests...${NC}"
kubectl apply -f k8s/

echo -e "${GREEN}==> 5.5. restarting deployments...${NC}"
kubectl rollout restart deployment/users-service
kubectl rollout restart deployment/learn-service
kubectl rollout restart deployment/ai-service
kubectl rollout restart deployment/gateway
kubectl rollout restart deployment/frontend

echo -e "${GREEN}==> 6. waiting for gateway & frontend...${NC}"
kubectl rollout status deployment/frontend --timeout=120s
kubectl rollout status deployment/gateway --timeout=120s

echo -e "${GREEN}==> 7. cleaning up old port-forwards...${NC}"
pkill -f "kubectl port-forward" 2>/dev/null || true

echo -e "${GREEN}==> 8. port-forwarding...${NC}"
echo -e "${YELLOW}• frontend: http://localhost:5173${NC}"
echo -e "${YELLOW}• gateway:  http://localhost:8080${NC}"

kubectl port-forward svc/frontend 5173:5173 &
kubectl port-forward svc/gateway 8080:8080 &
