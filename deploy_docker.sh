#!/usr/bin/env bash
set -e

GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${GREEN}==> docker-compose up${NC}"
docker compose up -d --build

echo -e "${GREEN}==> container's status:${NC}"
docker compose ps
