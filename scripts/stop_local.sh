#!/usr/bin/env bash
cd "$(dirname "$0")/.."
if [ -f logs/pids.txt ]; then
  while read -r pid; do kill "$pid" 2>/dev/null || true; done < logs/pids.txt
  rm -f logs/pids.txt
  echo "nos encerrados."
else
  echo "nenhum pid registrado."
fi
