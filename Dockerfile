# Two stages: the build stage installs the package, the runtime image carries
# only the result. Plain `pip install` on purpose — `uv` belongs in the
# development environment, not in a container image.
#
# The image serves the standalone deployment shape (src/.../standalone.py). A
# consumer that embeds the router instead builds its own image and does not use
# this file.
# ONE PLACE FOR THE PYTHON VERSION. Both stages and the copied path below are
# derived from this argument, because they have to agree: `pip install` puts the
# package under the interpreter's own directory, and the runtime stage copies
# from exactly there.
#
# Keeping them in three separate literals is what broke the build on 2026-09-13:
# Renovate raised both `FROM python:3.13-slim` lines to 3.14 -- correctly, that
# is its job -- while the hard-coded `/usr/local/lib/python3.13/site-packages`
# in the COPY stayed behind. The build then failed with
#
#   failed to compute cache key: "/usr/local/lib/python3.13/site-packages": not found
#
# and it failed only at the next build, not at the merge. The comment that used
# to sit above that COPY said "changing the base image tag means changing this
# path" -- a note asking a human to remember something a machine had just
# changed. This argument removes the need to remember.
#
# renovate: datasource=docker depName=python versioning=docker
ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}-slim AS build
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src

# hatch-vcs derives the version from git metadata, and .git is deliberately not
# part of the build context — without a value the build fails with
# "setuptools-scm was unable to detect version for /app". CI passes the tag
# being built; the default keeps a plain `docker build` working.
ARG SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0.dev0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

# [kafka] is the only implemented backend, and standalone.py registers exactly
# it — installing the package without the extra would yield an image whose
# every request ends in 503.
# [observability] alongside [kafka]: standalone.py calls install_observability(),
# so without the extra the image fails at import — deliberately, rather than
# running a service the estate cannot see. It is what gives the container log its
# JSON lines and the spans their trace ids.
RUN pip install --no-cache-dir ".[kafka,observability]" uvicorn

FROM python:${PYTHON_VERSION}-slim
# An ARG declared before the first FROM is outside every build stage; naming it
# again here brings it into this one. Without this line the substitution below
# would silently expand to an empty string, and the COPY would look for
# /usr/local/lib/python/site-packages.
ARG PYTHON_VERSION
COPY --from=build /usr/local/lib/python${PYTHON_VERSION}/site-packages /usr/local/lib/python${PYTHON_VERSION}/site-packages
COPY --from=build /usr/local/bin /usr/local/bin
RUN useradd --create-home --uid 10001 app
WORKDIR /app
USER app

ARG HTTP_PORT=8091
ENV HTTP_PORT=${HTTP_PORT}
EXPOSE ${HTTP_PORT}

# `exec`, so that uvicorn replaces the shell and becomes PID 1. Without it the
# shell stays PID 1, does not forward SIGTERM, and `docker stop` waits out its
# grace period before killing the container -- measured: 10s and no graceful
# shutdown at all, which would skip the lifespan and leave the Kafka producer
# to be torn down mid-flight. The shell is still needed for `$HTTP_PORT`.
#
# --proxy-headers: the service always runs behind a reverse proxy, and the
# signature check does not depend on the client address, but the access log
# should show the real one.
CMD ["sh", "-c", "exec uvicorn edutap.webhook_heidi.standalone:app --proxy-headers --host 0.0.0.0 --port $HTTP_PORT"]
