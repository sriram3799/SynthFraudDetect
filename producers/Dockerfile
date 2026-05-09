FROM python:3.11-slim

WORKDIR /app

# Install system deps needed by confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy schema files first so they are a separate layer (rarely change)
COPY schema/ ./schema/

# Copy producer source
COPY config.py generator.py kafka_producer.py ./

ENV PYTHONUNBUFFERED=1
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092

ENTRYPOINT ["python", "kafka_producer.py"]
CMD ["--tps", "25000", "--seed", "42"]
