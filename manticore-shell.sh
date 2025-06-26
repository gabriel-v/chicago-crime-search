#!/bin/bash

set -ex

export COMPOSE_PROJECT_NAME=chicago_crimes_search
cd "$( dirname "${BASH_SOURCE[0]}" )"
cd docker

docker compose exec -it manticore mysql -h 127.0.0.1 -P 9306 -u root --password=pass db 
