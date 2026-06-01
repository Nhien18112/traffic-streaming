FROM python:3.9-slim AS ml-base

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Shared runtime stack used by all ML services
RUN pip install --no-cache-dir kafka-python numpy torch torch_geometric

FROM ml-base AS ai-worker

RUN pip install --no-cache-dir ultralytics opencv-python-headless requests

CMD ["python", "/app/src/ai_worker/main.py"]

FROM ml-base AS gru-inference

CMD ["python", "/app/src/mlops/gru_inference.py"]

FROM ml-base AS gru-finetuner

CMD ["python", "/app/src/mlops/gru_finetuner.py"]
