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

echo -e "${GREEN}==> 3. building containers...${NC}"
docker build -t frontend:latest ./frontend
docker build -t gateway:latest ./gateway
docker build -t users-service:latest ./users-service
docker build -t learn-service:latest ./learn-service
docker build -t ai-service:latest ./ai-service

echo -e "${GREEN}==> 4. applying kubernetes-manifests...${NC}"
kubectl apply -f k8s/

echo -e "${GREEN}==> 5. waiting for gatewat & frontend...${NC}"
kubectl rollout status deployment/frontend --timeout=120s
kubectl rollout status deployment/gateway --timeout=120s

echo -e "${GREEN}==> 6. port-forwarding...${NC}"
echo -e "${YELLOW}• frontend: http://localhost:5173${NC}"
echo -e "${YELLOW}• gateway:  http://localhost:8080${NC}"

kubectl port-forward svc/frontend 5173:5173 > /dev/null 2>&1 &
kubectl port-forward svc/gateway 8080:8080 > /dev/null 2>&1 &

wait
