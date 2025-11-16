FROM registry.access.redhat.com/ubi9/python-311

WORKDIR /app

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy server code
COPY server.py .

# BigQuery uses this env var to find credentials JSON
ENV GOOGLE_APPLICATION_CREDENTIALS=/var/secrets/gcp/key.json

CMD ["python", "server.py"]
