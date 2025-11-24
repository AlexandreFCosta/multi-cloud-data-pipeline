# Multi-Cloud Data Pipeline Framework - Docker Image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ /app/src/
COPY examples/ /app/examples/
COPY setup.py /app/

# Install the package
RUN pip install -e .

# Create data directories
RUN mkdir -p /data/input /data/output /data/checkpoints

# Set environment variables
ENV PYTHONPATH=/app/src:$PYTHONPATH
ENV SPARK_HOME=/usr/local/lib/python3.9/site-packages/pyspark

# Expose ports (if needed for monitoring)
EXPOSE 4040 8080

# Default command
CMD ["python", "-m", "multicloud_pipeline"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import multicloud_pipeline; print('OK')" || exit 1

# Labels
LABEL maintainer="Alexandre <your.email@example.com>"
LABEL version="1.0.0"
LABEL description="Multi-Cloud Data Pipeline Framework for Azure and GCP"
