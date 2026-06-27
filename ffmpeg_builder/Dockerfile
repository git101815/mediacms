FROM nvidia/cuda:12.4.1-devel-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

ARG FFMPEG_VERSION=n7.1.1
ARG NV_CODEC_HEADERS_VERSION=n12.2.72.0

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

RUN git clone --depth 1 --branch ${NV_CODEC_HEADERS_VERSION} https://github.com/FFmpeg/nv-codec-headers.git \
    && cd nv-codec-headers \
    && make install

RUN git clone --depth 1 --branch ${FFMPEG_VERSION} https://github.com/FFmpeg/FFmpeg.git ffmpeg

WORKDIR /build/ffmpeg

RUN ./configure \
        --prefix=/opt/ffmpeg \
        --enable-gpl \
        --enable-nonfree \
        --enable-nvenc \
        --enable-cuda-nvcc \
        --enable-libx264 \
        --enable-libx265 \
        --enable-libsvtav1 \
        --extra-cflags=-I/usr/local/cuda/include \
        --extra-ldflags=-L/usr/local/cuda/lib64 \
    && make -j"$(nproc)" \
    && make install

RUN /opt/ffmpeg/bin/ffmpeg -hide_banner -encoders | grep av1_nvenc
RUN /opt/ffmpeg/bin/ffmpeg -hide_banner -encoders | grep h264_nvenc
RUN /opt/ffmpeg/bin/ffmpeg -hide_banner -encoders | grep libsvtav1
RUN /opt/ffmpeg/bin/ffprobe -hide_banner -version