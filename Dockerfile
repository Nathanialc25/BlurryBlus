FROM apache/airflow:3.0.3

USER root

# Install OpenSSL, CA certificates, and essential build tools.
# These are required for SSL support in Python and for building any Python packages
# that need to compile C extensions (like some database drivers).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openssl \
    ca-certificates \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libffi-dev && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
