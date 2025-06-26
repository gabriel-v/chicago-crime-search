#!/bin/bash

set -ex

cd "$( dirname "${BASH_SOURCE[0]}" )"

(
    cd docker
    if ! [ -f manticore.conf ]; then
        cp manticore.conf.initial manticore.conf
    else
        echo "manticore.conf already exists"
        touch manticore.conf
    fi
)

(
    unset COMPOSE_PROJECT_NAME
    cd docker/lib/superset
    export TAG=4.1.2
     docker compose -f docker-compose-image-tag.yml up -d
    #  for x in superset_app superset_worker superset_worker_beat; do docker exec -it $x pip install clickhouse-connect psycopg2-binary; done
    #  docker compose restart
)

(
    export COMPOSE_PROJECT_NAME=chicago_crimes_search
    cd docker
    docker compose up -d
)