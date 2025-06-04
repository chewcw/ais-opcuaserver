# syntax=docker/dockerfile:1.7
FROM python:3.13-slim AS builder

WORKDIR /app

ARG TARGETARCH
ARG TARGETVARIANT

# Install uv
RUN pip install --no-cache-dir uv

# Add runtime dependencies including openssl for cert generation and postgresql dev for building psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libpq-dev \
    postgresql-client \
    build-essential \
    libffi-dev \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Add new user first (before any COPY commands that use it)
RUN addgroup --gid 1000 appuser && \
    adduser appuser --uid 1000 --gid 1000 --home /home/appuser --shell /bin/bash --disabled-password

# Copy dependency files first for better caching
COPY --link README.md uv.lock ./
COPY --link pyproject.${TARGETARCH}${TARGETVARIANT}.toml pyproject.toml

# Install dependencies (this layer will be cached independently)
RUN python -m uv lock && python -m uv sync --locked

# Copy application source code with preserved structure
COPY --link ./src ./src

# Copy entrypoint script
COPY --link docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set ownership for the app directory
RUN chown -R appuser:appuser /app

# Switch to app user
USER appuser

# Create necessary directories with preserved structure
RUN mkdir -p /app/src/opcua_server/plugins/aissens_sqldb/data /app/certs

# Define volume mount points for configuration files
VOLUME /app/src/opcua_server/config
VOLUME /app/src/opcua_server/plugins/aissens_sqldb/config
# Define volume mount points for certificates
VOLUME /app/certs

# Expose the OPC UA port
EXPOSE 4840

# Use entrypoint script to generate certificates and start server
ENTRYPOINT ["docker-entrypoint.sh"]
# Default command
CMD ["python", "-m", "uv", "run", "opcua_server"]
