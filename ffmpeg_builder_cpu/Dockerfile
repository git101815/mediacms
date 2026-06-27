FROM python:3.13.5-bookworm

ENV DEBIAN_FRONTEND=noninteractive

ARG FFMPEG_VERSION=n7.1.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        autoconf \
        automake \
        build-essential \
        ca-certificates \
        cmake \
        git \
        libtool \
        nasm \
        pkg-config \
        texinfo \
        wget \
        yasm \
        zlib1g-dev \
        libnuma-dev \
        libx264-dev \
        libx265-dev \
        libsvtav1-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

RUN git clone --depth 1 --branch ${FFMPEG_VERSION} https://github.com/FFmpeg/FFmpeg.git ffmpeg

WORKDIR /build/ffmpeg

RUN ./configure \
        --prefix=/opt/ffmpeg \
        --enable-gpl \
        --enable-libx264 \
        --enable-libx265 \
        --enable-libsvtav1 \
    && make -j"$(nproc)" \
    && make install

RUN /opt/ffmpeg/bin/ffmpeg -hide_banner -encoders | grep libsvtav1
RUN /opt/ffmpeg/bin/ffmpeg -hide_banner -encoders | grep libx264
RUN /opt/ffmpeg/bin/ffprobe -hide_banner -version