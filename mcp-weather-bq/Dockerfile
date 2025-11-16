FROM registry.access.redhat.com/ubi9/python-311

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy server and key
COPY server.py .
COPY gcp-sa.json .

CMD ["python", "server.py"]