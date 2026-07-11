FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements-docker.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY druck/ druck/
COPY run_web.py run_auto.py run_report.py config.yaml ./

# Create directories for persistent data. Keep the application's existing
# /app/trade_log.db path while storing the database on a Compose volume.
RUN mkdir -p output .cache state \
    && ln -s /app/state/trade_log.db /app/trade_log.db

EXPOSE 8000

CMD ["python", "run_web.py", "--host", "0.0.0.0"]
