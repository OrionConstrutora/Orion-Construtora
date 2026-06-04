FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ldm_webhook.py .
COPY ldm_ids.json .

EXPOSE 8000

CMD ["/bin/sh", "-c", "uvicorn ldm_webhook:app --host 0.0.0.0 --port ${PORT:-8000}"]
