FROM python:3.12-slim
# Pinning ffmpeg's exact Debian package version would break the build the
# next time python:3.12-slim's floating tag picks up a newer Debian point
# release and that exact version ages out of the repo -- not worth the
# fragility for a codec library where we don't depend on specific version
# behavior.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Matches the ScaledJob/sweeper pod securityContext's runAsUser (Module 14) --
# set here too so runAsNonRoot is satisfied under plain `docker run` as well.
USER 10001
CMD ["python", "main.py"]
