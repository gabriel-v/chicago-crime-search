#!/bin/bash

set -ex

cd "$( dirname "${BASH_SOURCE[0]}" )"


while true; do
    uv run search_demo.py || sleep 1
    sleep 0.5
done