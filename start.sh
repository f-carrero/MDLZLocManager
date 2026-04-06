#!/bin/bash
PORT="${DATABRICKS_APP_PORT:-8501}"
exec streamlit run app/streamlit_app.py --server.port "$PORT" --server.address 0.0.0.0 --server.enableCORS false --server.enableXsrfProtection false
