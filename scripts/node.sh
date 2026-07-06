#!/usr/bin/env bash

cd "$(dirname "$0")/.."
python run_node.py --id "${1:?informe o id do no}" --config config.local.json --mode interactive
