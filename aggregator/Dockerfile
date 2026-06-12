# =============================================================================
# Dockerfile — Aggregator / Worker Service
# =============================================================================
# Multi-purpose image: runs as API server (default) or worker (via command override)
# =============================================================================

FROM python:3.11-slim

# Prevent Python from writing .pyc and enable unbuffered output for logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies for asyncpg (PostgreSQL client library)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser
USER appuser

# Default: run FastAPI server on port 3000
EXPOSE 3000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3000", "--log-level", "info"]
