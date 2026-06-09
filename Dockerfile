FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY remote_server.py .

# MCP_TOKEN must be supplied at runtime (set it in your host's env vars).
ENV MCP_TOKEN=""

# Most hosts (Render, Railway, Koyeb, Fly) inject $PORT. Default 8000 locally.
CMD ["sh", "-c", "uvicorn remote_server:app --host 0.0.0.0 --port ${PORT:-8000}"]
