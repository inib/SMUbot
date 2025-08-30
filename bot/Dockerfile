FROM python:3.12-slim
WORKDIR /bot
ENV PYTHONUNBUFFERED=1
COPY bot/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY bot/bot_app.py ./bot_app.py
COPY bot/commands.yml ./commands.yml
COPY bot/messages.yml ./messages.yml
ENV COMMANDS_FILE=/bot/commands.yml
ENV BOT_MESSAGES_PATH=/bot/messages.yml
CMD ["python", "-u", "bot_app.py"]
