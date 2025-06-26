#!/bin/bash

set -ex

cd "$( dirname "${BASH_SOURCE[0]}" )"

docker rm -f $(docker ps -qa) || true
docker volume rm -f $(docker volume ls -q) || true
bash start-docker.sh
uv run process_0_reset_databases.py
uv run process_1_load_csv.py