FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements-docker.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY druck/ druck/
COPY run_web.py run_auto.py run_report.py config.yaml ./

# Create directories for persistent data
RUN mkdir -p output .cache

EXPOSE 8000

CMD ["python", "run_web.py"]
