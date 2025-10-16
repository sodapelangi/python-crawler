# Dockerfile
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY app ./app

RUN useradd -m appuser
USER appuser

ENV PORT=8080
CMD ["uvicorn", "app.server:app", "--host", "0.0.0.0", "--port", "8080"]
