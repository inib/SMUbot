FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY backend_app.py ./backend_app.py
COPY run.sh ./run.sh
RUN chmod +x run.sh
VOLUME ["/data"]
EXPOSE ${API_PORT}
CMD ["sh", "-c", "uvicorn backend_app:app --host 0.0.0.0 --port ${API_PORT}"]
