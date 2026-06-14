# syntax=docker/dockerfile:1

FROM python:3.11-slim

WORKDIR /app

# Install dependencies first so this layer is cached separately from source
COPY telemetry/requirements.txt ./telemetry/requirements.txt
COPY infra/requirements.txt ./infra/requirements.txt
RUN pip install --no-cache-dir \
    -r telemetry/requirements.txt \
    -r infra/requirements.txt

COPY telemetry/ ./telemetry/
COPY infra/ ./infra/

# Default command runs the API; scoring-engine overrides this in docker-compose.yml
CMD ["python3", "-u", "-m", "uvicorn", "telemetry.api:app", "--host", "0.0.0.0", "--port", "8000"]
