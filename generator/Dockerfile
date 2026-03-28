FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy generator script
COPY generate_raw.py .

# Default: daily mode (overridden by Kestra)
CMD ["python", "generate_raw.py", "daily"]