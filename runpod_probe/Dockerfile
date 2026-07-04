ARG FFMPEG_BUILDER_IMAGE=aad7xppz4fd4r8emr/cf-ffmpeg-builder:cuda12.4-ffmpeg7.1.1-nvcodec12.2.72.0

FROM ubuntu:22.04 AS nvscope_builder

ARG NVSCOPE_REPO=https://github.com/MadiatorLabs/nvscope.git
ARG NVSCOPE_REF=main

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch "${NVSCOPE_REF}" "${NVSCOPE_REPO}" /src/nvscope \
    && make -C /src/nvscope \
    && make -C /src/nvscope install DESTDIR=/nvscope-root

FROM ${FFMPEG_BUILDER_IMAGE} AS ffmpeg_builder

FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility,video

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        python3 \
        python3-pip \
        libgomp1 \
        libnuma1 \
        libx264-163 \
        libx265-199 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ffmpeg_builder /opt/ffmpeg /opt/ffmpeg

COPY --from=nvscope_builder /nvscope-root/usr/local/lib/nvscope/libnvscope.so /usr/local/lib/nvscope/libnvscope.so
COPY --from=nvscope_builder /nvscope-root/usr/local/bin/nvscope /usr/local/bin/nvscope
COPY --from=nvscope_builder /nvscope-root/usr/local/bin/nvscope-probe /usr/local/bin/nvscope-probe

ENV PATH="/opt/ffmpeg/bin:${PATH}"
ENV NVSCOPE_LIB=/usr/local/lib/nvscope/libnvscope.so
ENV NVSCOPE_BIN=/usr/local/bin/nvscope

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY handler.py /app/handler.py

CMD ["python3", "-u", "/app/handler.py"]