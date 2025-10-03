#################################################################
# Base image
#################################################################
FROM denoland/deno:2.5.3 AS worker

# # https://gist.github.com/squarebracket/e719069522436873bc6f13efb359cac9
# RUN cp /etc/ssl/certs/ca-certificates.crt /etc/ssl/cert.pem
RUN apt-get update && apt-get -y install \
	curl \
	gnupg \
	unzip

# Set up nvidia repository for GPU support in docker
RUN curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
	|  gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
	&& curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
	sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
	tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

RUN apt-get update && apt-get -y install \
	ca-certificates \
	nvidia-container-toolkit \
	fuse-overlayfs \
	uuid-runtime && \
	install -m 0755 -d /etc/apt/keyrings && \
	curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc && \
	chmod a+r /etc/apt/keyrings/docker.asc && \
	echo \
	"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
	$(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
	tee /etc/apt/sources.list.d/docker.list > /dev/null && \
	apt-get update && \
	apt-get --yes install \
	docker-ce \
	docker-buildx-plugin && \
	rm -rf /var/lib/apt/lists/*

VOLUME /var/lib/containers

WORKDIR /app

COPY ./deno.json ./deno.json
COPY ./deno.lock ./deno.lock

# Deno will otherwise complain about missing workspace files
RUN mkdir -p ./app/api ./app/cli ./app/test
RUN touch ./app/api/deno.json ./app/cli/deno.json ./app/test/deno.json

# Copy worker files
COPY ./app/worker/src ./app/worker/src
COPY ./app/worker/deno.json ./app/worker/deno.json
COPY ./app/worker/mod.json ./app/worker/mod.json

# Copy shared files
COPY ./app/shared/src ./app/shared/src
COPY ./app/shared/deno.json ./app/shared/deno.json

# Copy assets
# COPY ./app/browser/dist ./app/browser/dist

RUN deno cache .

WORKDIR /app/app/worker

ARG VERSION=cache
ENV VERSION=$VERSION

LABEL container.mtfm.io=worker

ENTRYPOINT ["deno", "run", "--allow-all", "--unstable-kv", "--unstable-cron", "src/cli.ts"]

# The standalone worker contains its own copy of dockerd, so it doesn't need to rely
# on access to the host, and can run in environments where a docker socket isn't provided.
FROM worker AS worker-standalone

RUN apt-get update && \
	apt-get --yes install \
	docker-ce \
	containerd.io && \
	rm -rf /var/lib/apt/lists/*

COPY ./app/worker/daemon.json /etc/docker/daemon.json

ENTRYPOINT ["deno", "run", "--allow-all", "--unstable-kv", "--unstable-cron", "src/cli.ts"]

