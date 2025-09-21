# 9/9 NC revist this later, want a bit better understanding of zlib1g-dev & libffi-dev

#base image to get things going
FROM apache/airflow:3.0.3

#switch to root user to install sys packages
USER root

# updates the package list for apt(the ubuntu package manager)
# installs w/o asking for confirmation, avoiding the recommended installs, installs the following
# ca-certificates- install openssl - provides cryptographic ssl or tls for http request! need this!
# build-essential- installs trusted CA certificates. Allows container to verify SSL certificates when connecting to external services (like APIs, databases, etc.)
# Installs meta-package that includes GCC, g++
# libssl-dev- Installs OpenSSL development files (headers and libraries).
# next two are also just good to have for ssl, and python things- should look into this again
#  apt-get autoremove -yqq --purge && \ - queitly removig packages, and configurations that are no longer needed
# apt-get clean && \ - clears local repo of retrieved packages files to free space.
# rm -rf /var/lib/apt/lists/* - removes package lists downloaded by apt-get update to help reduce image size

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
 
# switching back to airlfow user for better security pracitce.
USER airflow

# install python dependencies
RUN pip install --no-cache-dir rapidfuzz