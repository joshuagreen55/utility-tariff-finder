# API image for production / Google Cloud VM (docker compose)
FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    tesseract-ocr \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create non-root user for running the app
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser \
    && mkdir -p /app/logs && chown -R appuser:appuser /app

# Install Playwright's Chromium browser + OS deps
# Set PLAYWRIGHT_BROWSERS_PATH so appuser can find the binaries
ENV PLAYWRIGHT_BROWSERS_PATH=/app/.playwright-browsers
RUN playwright install --with-deps chromium \
    && chown -R appuser:appuser /app/.playwright-browsers

COPY --chown=appuser:appuser app ./app
COPY --chown=appuser:appuser alembic ./alembic
COPY --chown=appuser:appuser alembic.ini .
COPY --chown=appuser:appuser scripts ./scripts

USER appuser

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
