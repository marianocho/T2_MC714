#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs
: > logs/pids.txt
for id in 1 2 3 4; do
  python run_node.py --id "$id" --config config.local.json --mode demo > "logs/node${id}.log" 2>&1 &
  echo $! >> logs/pids.txt
done
echo "4 nos rodando em modo demo (roteiro de ~35s)."
echo "Acompanhar ao vivo:    tail -f logs/node*.log"
echo "Visao cronologica:     python scripts/merge_logs.py"
echo "Parar tudo:            bash scripts/stop_local.sh"
