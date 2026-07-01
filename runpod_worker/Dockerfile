ARG FFMPEG_BUILDER_IMAGE=aad7xppz4fd4r8emr/cf-ffmpeg-builder:cuda12.4-ffmpeg7.1.1-nvcodec12.2.72.0

FROM ${FFMPEG_BUILDER_IMAGE} AS ffmpeg_builder

FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility,video

ARG BENTO4_VERSION=1-6-0-641
ARG BENTO4_ZIP=Bento4-SDK-${BENTO4_VERSION}.x86_64-unknown-linux.zip

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        unzip \
        python3 \
        python3-pip \
        libgomp1 \
        libnuma1 \
        libx264-163 \
        libx265-199 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ffmpeg_builder /opt/ffmpeg /opt/ffmpeg

RUN curl -fsSL "https://www.bok.net/Bento4/binaries/${BENTO4_ZIP}" -o /tmp/bento4.zip \
    && unzip -q /tmp/bento4.zip -d /tmp \
    && mv "/tmp/Bento4-SDK-${BENTO4_VERSION}.x86_64-unknown-linux" /opt/bento4 \
    && rm -f /tmp/bento4.zip

ENV PATH="/opt/ffmpeg/bin:/opt/bento4/bin:${PATH}"

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY handler.py /app/handler.py

CMD ["python3", "-u", "/app/handler.py"]