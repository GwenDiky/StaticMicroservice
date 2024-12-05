#!/bin/sh
trap 'exit' INT TERM
trap 'kill 0' EXIT

echo "Waiting for MongoDB..."

until nc -z mongodb 27017; do
  echo "Waiting for Mongo..."
  sleep 1
done

echo "MongoDB is ready."

export PATH="/src/.venv/bin:$PATH"

exec uvicorn src.main:app --host 0.0.0.0 --port 8084 --reload
