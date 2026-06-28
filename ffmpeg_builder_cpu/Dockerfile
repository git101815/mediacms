FROM python:3.13.5-bookworm

ENV DEBIAN_FRONTEND=noninteractive
ENV PKG_CONFIG_PATH="/usr/local/lib/pkgconfig:/usr/local/lib64/pkgconfig:/opt/svt-av1/lib/pkgconfig"
ENV LD_LIBRARY_PATH="/usr/local/lib:/usr/local/lib64:/opt/svt-av1/lib"

ARG FFMPEG_VERSION=n7.1.1
ARG SVT_AV1_VERSION=v2.3.0

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
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

RUN git clone --depth 1 --branch ${SVT_AV1_VERSION} https://gitlab.com/AOMediaCodec/SVT-AV1.git svt-av1 \
    && cmake -S svt-av1 -B svt-av1/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/opt/svt-av1 \
        -DBUILD_SHARED_LIBS=ON \
    && cmake --build svt-av1/build -j"$(nproc)" \
    && cmake --install svt-av1/build

RUN pkg-config --modversion SvtAv1Enc

RUN git clone --depth 1 --branch ${FFMPEG_VERSION} https://github.com/FFmpeg/FFmpeg.git ffmpeg

WORKDIR /build/ffmpeg

RUN ./configure \
        --prefix=/opt/ffmpeg \
        --enable-gpl \
        --enable-libx264 \
        --enable-libx265 \
        --enable-libsvtav1 \
        --extra-cflags="-I/opt/svt-av1/include" \
        --extra-ldflags="-L/opt/svt-av1/lib" \
    && make -j"$(nproc)" \
    && make install

RUN /opt/ffmpeg/bin/ffmpeg -hide_banner -encoders | grep libsvtav1
RUN /opt/ffmpeg/bin/ffmpeg -hide_banner -encoders | grep libx264
RUN /opt/ffmpeg/bin/ffprobe -hide_banner -version