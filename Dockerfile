# Use the same Apache Airflow version as in your compose file
FROM apache/airflow:3.0.3

# Switch to root to install system packages
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

# Switch back to the airflow user for security
USER airflow

# Note: The providers (postgres and smtp) are installed via 
# _PIP_ADDITIONAL_REQUIREMENTS in your docker-compose.yml environment.
# They will be installed during container initialization with the proper
# SSL libraries now available for compilation.