FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install curl and tar to fetch/extract daprd
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates tar \
    && rm -rf /var/lib/apt/lists/*

# Install Dapr runtime (daprd) v1.13.6 (auto-detect arch)
ARG DAPR_VERSION=1.13.6
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "${arch}" in amd64|arm64) ;; *) echo "Unsupported arch: ${arch}"; exit 1;; esac; \
    curl -fsSL -o /tmp/daprd.tar.gz "https://github.com/dapr/dapr/releases/download/v${DAPR_VERSION}/daprd_linux_${arch}.tar.gz"; \
    tar -xzf /tmp/daprd.tar.gz -C /usr/local/bin daprd; \
    rm /tmp/daprd.tar.gz; \
    chmod +x /usr/local/bin/daprd; \
    /usr/local/bin/daprd --version || true

WORKDIR /app

# Copy source and install project dependencies
COPY . /app

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir .

# Provide Dapr components inside image
COPY components-docker /components

# Entrypoint script to start daprd + app
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 3000 3500 50001

CMD ["/entrypoint.sh"]
