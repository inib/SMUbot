FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY backend_app.py ./backend_app.py
COPY run.sh ./run.sh
RUN chmod +x run.sh
VOLUME ["/data"]
# The API always listens on port 7070 inside the container.
EXPOSE 7070
CMD ["./run.sh"]
