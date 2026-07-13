#!/bin/zsh
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <iterations>"
  exit 1
fi

for ((i=1; i<=$1; i++)); do
  echo "Loop #$i"

  ready_count=$(BD_JSON_ENVELOPE=1 bd ready --json | jq '.data | length')
  if [ "$ready_count" -eq 0 ]; then
    echo "No tasks remaining. PRD complete after $i iterations."
    exit 0
  fi

  result=$(claude -p "/implement-next-task")
  echo "$result"
done