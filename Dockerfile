FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
COPY *.json ./

# klines/ and logs/ are expected to be mounted as volumes so data
# survives container restarts and is visible to other containers.
VOLUME ["/app/klines", "/app/logs"]

EXPOSE 8000

# Override CMD or set BINANCE_MODE env var to change behaviour:
#   BINANCE_MODE=init   — download all history, then stream + serve API
#   BINANCE_MODE=stream — stream + serve API (history already populated)
#   BINANCE_MODE=api    — serve API only (read-only replica)
CMD ["python", "api.py"]
