ARG FFMPEG_BUILDER_IMAGE

FROM ${FFMPEG_BUILDER_IMAGE} AS ffmpeg_builder

FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/opt/ffmpeg/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/cuda/lib64:${LD_LIBRARY_PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        python3 \
        python3-pip \
        libgomp1 \
        libnuma1 \
        libx264-163 \
        libx265-199 \
        libsvtav1enc1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ffmpeg_builder /opt/ffmpeg /opt/ffmpeg

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY handler.py /app/handler.py

CMD ["python3", "-u", "/app/handler.py"]