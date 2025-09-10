#!/bin/bash
cd /home/kavia/workspace/code-generation/healthcheckscipt-1025-1066/HealthCheckScriptContainer
source venv/bin/activate
flake8 .
LINT_EXIT_CODE=$?
if [ $LINT_EXIT_CODE -ne 0 ]; then
  exit 1
fi

