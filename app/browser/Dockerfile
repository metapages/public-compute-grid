FROM denoland/deno:alpine-2.5.3

RUN apk --no-cache --update add \
    bash \
    curl \
    git \
    jq \
    npm \
    openssh

# https://gist.github.com/squarebracket/e719069522436873bc6f13efb359cac9
RUN cp /etc/ssl/certs/ca-certificates.crt /etc/ssl/cert.pem

# Needs community repo
RUN apk add --no-cache -X https://dl-cdn.alpinelinux.org/alpine/edge/community \
    sd

###############################################################################
# justfile for running commands, you will mostly interact with just https://github.com/casey/just
###############################################################################
RUN apk add --no-cache -X https://dl-cdn.alpinelinux.org/alpine/edge/community \
    just
# alias "j" to just, it's just right there index finger
RUN echo -e '#!/bin/bash\njust "$@"' > /usr/bin/j && \
    chmod +x /usr/bin/j
ENV JUST_SUPPRESS_DOTENV_LOAD_WARNING=1

###############################################################################
# watchexec for fast development reloading
###############################################################################
RUN apk add --no-cache -X https://dl-cdn.alpinelinux.org/alpine/edge/community \
    watchexec
