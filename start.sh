#!/bin/bash
export FORCE_COLOR=1
export PYTHONUNBUFFERED=1
echo "🚀 Démarrage de SSDv2 sur http://localhost:8080"
poetry run uvicorn src.main:app --host 0.0.0.0 --port 8080 --log-level debug
