#!/usr/bin/env bash
set -euo pipefail
export ADMIN_TOKEN=${ADMIN_TOKEN:-change-me}

# one-shot reset when you want a fresh DB
if [ "${RESET_DB:-0}" = "1" ]; then
  rm -f /data/db.sqlite
fi

# create tables if missing
python - <<'PY'
import backend_app
backend_app.Base.metadata.create_all(bind=backend_app.engine)
print("DB ready at", backend_app.DB_URL)
PY

uvicorn backend_app:app --host 0.0.0.0 --port 7070
