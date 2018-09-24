#!/usr/bin/env bash
set -euo pipefail

cd /root/luffer

echo ufw
ufw disable

echo docker-compose build
docker-compose build
