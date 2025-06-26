#!/bin/bash

set -ex

echo "Updating manticore config for $1"

cp /docker-mounted-manticore.conf /etc/manticoresearch/manticore.conf

echo "Manticore config updated"

time indexer --rotate $1

echo "Notify of rotate"
kill -SIGHUP 1