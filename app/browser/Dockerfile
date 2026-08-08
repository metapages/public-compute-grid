FROM ubuntu:22.04

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and install basic packages
RUN apt-get update && apt-get install -y \
    bash \
    curl \
    git \
    jq \
    openssh-client \
    wget \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

###############################################################################
# Install latest Node.js (>= 21) from NodeSource
###############################################################################
RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - && \
    apt-get install -y nodejs

###############################################################################
# Install Deno
# Pinned to match the version used everywhere else in the repo (the other
# Dockerfiles, the compose files, and the CI workflows). Deno 2.9 changed the
# setInterval return type, so an unpinned "latest" install here would drift
# away from what the code is typechecked against.
###############################################################################
RUN curl -fsSL https://deno.land/install.sh | sh -s v2.9.3
ENV PATH="/root/.deno/bin:${PATH}"

###############################################################################
# Install sd (sed alternative)
###############################################################################
# Pinned: the URL must name a version that exists as an asset of that exact
# release. Using /releases/latest/download/ with a hardcoded version breaks the
# moment upstream cuts a new release, which is what happened to v1.0.0.
ARG SD_VERSION=v1.1.0
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "arm64" ]; then \
        SD_ARCH="aarch64-unknown-linux-musl"; \
    else \
        SD_ARCH="x86_64-unknown-linux-gnu"; \
    fi && \
    curl -fL "https://github.com/chmln/sd/releases/download/${SD_VERSION}/sd-${SD_VERSION}-${SD_ARCH}.tar.gz" \
    | tar xz -C /tmp && \
    mv /tmp/sd-*/sd /usr/local/bin/ && \
    chmod +x /usr/local/bin/sd

###############################################################################
# Install just - justfile for running commands
# https://github.com/casey/just
###############################################################################
RUN curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin

# alias "j" to just, it's just right there index finger
RUN echo -e '#!/bin/bash\njust "$@"' > /usr/bin/j && \
    chmod +x /usr/bin/j
ENV JUST_SUPPRESS_DOTENV_LOAD_WARNING=1

###############################################################################
# Install watchexec for fast development reloading
###############################################################################
# Pinned to the release the asset name refers to — see the SD_VERSION note above.
ARG WATCHEXEC_VERSION=2.5.1
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "arm64" ]; then \
        WATCHEXEC_ARCH="aarch64-unknown-linux-musl"; \
    else \
        WATCHEXEC_ARCH="x86_64-unknown-linux-musl"; \
    fi && \
    curl -fL "https://github.com/watchexec/watchexec/releases/download/v${WATCHEXEC_VERSION}/watchexec-${WATCHEXEC_VERSION}-${WATCHEXEC_ARCH}.deb" -o /tmp/watchexec.deb && \
    dpkg -i /tmp/watchexec.deb && \
    rm /tmp/watchexec.deb