#!/bin/bash
# Load .env and run dbt command

# Activate virtual environment first
source venv/Scripts/activate

# Load environment variables (filter out empty lines and comments)
export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)

# Navigate to dbt project
cd airport_dbt

# Run dbt command
dbt "$@"