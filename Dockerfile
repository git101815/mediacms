FROM python:3.13.5-bookworm AS build-image

# Install system dependencies needed for Bento4
ARG BENTO4_VERSION=1-6-0-641
ARG BENTO4_ZIP=Bento4-SDK-${BENTO4_VERSION}.x86_64-unknown-linux.zip
ARG BENTO4_URL=https://www.bok.net/Bento4/binaries/${BENTO4_ZIP}

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends ca-certificates curl unzip && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get purge --auto-remove && \
    apt-get clean

# Install Bento4 in the specified location
RUN mkdir -p /home/mediacms.io/bento4 && \
    curl -fsSL --retry 5 --retry-delay 3 "${BENTO4_URL}" -o "/tmp/${BENTO4_ZIP}" && \
    unzip -q "/tmp/${BENTO4_ZIP}" -d /home/mediacms.io/bento4 && \
    mv "/home/mediacms.io/bento4/Bento4-SDK-${BENTO4_VERSION}.x86_64-unknown-linux/"* /home/mediacms.io/bento4/ && \
    rm -rf "/home/mediacms.io/bento4/Bento4-SDK-${BENTO4_VERSION}.x86_64-unknown-linux" && \
    rm -rf /home/mediacms.io/bento4/docs && \
    rm -f "/tmp/${BENTO4_ZIP}"

############ RUNTIME IMAGE ############
FROM python:3.13.5-bookworm AS runtime_image

SHELL ["/bin/bash", "-c"]

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV CELERY_APP='cms'
ENV VIRTUAL_ENV=/home/mediacms.io
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install runtime system dependencies
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y \
        ca-certificates \
        curl \
        supervisor \
        nginx \
        imagemagick \
        procps && \
    install -d /usr/share/postgresql-common/pgdg && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc && \
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update -y && \
    apt-get install --no-install-recommends -y postgresql-client-17 && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get purge --auto-remove -y && \
    apt-get clean
# Add Bento4
COPY --from=build-image /home/mediacms.io/bento4 /home/mediacms.io/bento4

# Set up virtualenv
RUN mkdir -p /home/mediacms.io/mediacms/{logs} && \
    cd /home/mediacms.io && \
    python3 -m venv "$VIRTUAL_ENV"

COPY requirements.txt requirements-dev.txt ./

ARG DEVELOPMENT_MODE=False

RUN pip install --no-cache-dir -r requirements.txt && \
    if [ "$DEVELOPMENT_MODE" = "True" ]; then \
        echo "Installing development dependencies..." && \
        pip install --no-cache-dir -r requirements-dev.txt; \
    fi && \
    python -c "import bip_utils; print(bip_utils.__version__)"

# Copy application files
COPY . /home/mediacms.io/mediacms
WORKDIR /home/mediacms.io/mediacms

COPY deploy/docker/policy.xml /etc/ImageMagick-6/policy.xml

# Set process control environment variables
ENV ENABLE_UWSGI='yes' \
    ENABLE_NGINX='yes' \
    ENABLE_CELERY_BEAT='yes' \
    ENABLE_CELERY_SHORT='yes' \
    ENABLE_CELERY_LONG='yes' \
    ENABLE_MIGRATIONS='yes'

EXPOSE 9000 80

RUN chmod +x ./deploy/docker/entrypoint.sh

ENTRYPOINT ["./deploy/docker/entrypoint.sh"]
CMD ["./deploy/docker/start.sh"]
