#!/bin/bash

set -e

export COMPOSE_PROJECT_NAME=chicago_crimes_search
cd "$( dirname "${BASH_SOURCE[0]}" )"
cd docker

docker compose exec -i clickhouse clickhouse-client -h localhost -u chicago_crimes_search --password chicago_crimes_search --database chicago_crimes_search "$@"