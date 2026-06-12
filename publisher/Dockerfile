# =============================================================================
# Dockerfile — Publisher Service
# =============================================================================

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser
USER appuser

CMD ["python", "publisher.py"]
