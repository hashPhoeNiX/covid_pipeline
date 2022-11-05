#!/bin/bash 

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

set -euo pipefail # causes the script to fail

base=docker-compose.yaml
override=docker-compose.override.yaml
prod=docker-compose.prod.yaml

if [ -f "$prod" ]; then
    echo $"Production compose file found!"
    exec docker compose -f $base -f $prod up -build
else
    exec docker compose up --build
fi