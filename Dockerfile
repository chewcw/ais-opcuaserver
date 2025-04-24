# Build stage
FROM python:3.13-slim AS builder

# Set working directory
WORKDIR /app

# Install build dependencies 
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements from pyproject.toml
COPY pyproject.toml uv.lock ./

# Install dependencies in site-packages
RUN pip install --no-cache-dir --prefix=/install -e .

# Runtime stage
FROM python:3.13-slim

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Add runtime dependencies including openssl for cert generation
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy application code
COPY ./main.py config.yaml ./
COPY ./src/server/ ./src/server/
COPY ./src/plugins/ ./src/plugins/

# Create necessary directories
RUN mkdir -p src/plugins/aissens_sqldb/data certs

# Create entrypoint script to generate certificates and start server
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Expose the OPC UA port
EXPOSE 4840

# Set Python to not write bytecode and not buffer output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Use entrypoint script to generate certificates and start server
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["python", "main.py"]

