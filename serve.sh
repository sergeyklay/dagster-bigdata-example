#!/bin/bash

# Export environment variables for Dagster development
export DAGSTER_HOME="$(pwd)/dagster"
export PYTHONPATH="$(pwd):$PYTHONPATH"

echo "Environment variables set:"
echo "DAGSTER_HOME=$DAGSTER_HOME"
echo "PYTHONPATH=$PYTHONPATH"

# Run the dagster dev server
uv run dagster dev --port 3333
