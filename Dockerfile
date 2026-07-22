FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends git curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# kubectl + gh CLI -- used by agora-persona-runner's kubectl_read/github_read
# tools (Agora Issues.md #3). Pinned versions, not "latest", so a rebuild
# months from now doesn't silently pick up a different major version.
# Every other consumer of this image pins its own @sha256 digest
# independently and is unaffected by this (or any) rebuild until/unless its
# own manifest is separately bumped to the new digest -- the RBAC grant and
# GITHUB_READONLY_TOKEN that make these binaries actually do anything are
# scoped only to agora-persona-runner's ServiceAccount/secret, so the
# binaries are simply inert on every other pod that happens to run this
# image.
ARG KUBECTL_VERSION=v1.36.2
RUN curl -fsSLo /usr/local/bin/kubectl \
    "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
    && chmod +x /usr/local/bin/kubectl

ARG GH_CLI_VERSION=2.96.0
RUN curl -fsSL \
    "https://github.com/cli/cli/releases/download/v${GH_CLI_VERSION}/gh_${GH_CLI_VERSION}_linux_amd64.tar.gz" \
    | tar -xz -C /usr/local --strip-components=1 "gh_${GH_CLI_VERSION}_linux_amd64/bin/gh"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
CMD ["python", "main.py"]
