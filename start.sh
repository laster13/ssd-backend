#!/bin/bash
echo "🚀 Démarrage de SSDv2 sur http://localhost:8080"
poetry run uvicorn src.main:app --host 0.0.0.0 --port 8080
