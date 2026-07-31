FROM ubuntu:24.04

ARG PG_VERSION=19beta2
ARG AWS_SDK_VERSION=1.11.360
ARG ARROW_VERSION=18.1.0

ENV DEBIAN_FRONTEND=noninteractive
ENV PG_CONFIG=/usr/local/pgsql/bin/pg_config
ENV AWS_SDK_ROOT=/opt/aws-sdk-cpp
ENV HDFS3_ROOT=/usr/local

# ── system packages ──────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl git \
    cmake ninja-build pkg-config \
    # PostgreSQL build deps
    bison flex libreadline-dev zlib1g-dev libssl-dev libxml2-dev \
    # Arrow apt source deps
    apt-transport-https gnupg lsb-release wget \
    # AWS SDK deps
    libcurl4-openssl-dev libssl-dev uuid-dev \
    # HDFS / Kerberos / SASL
    libkrb5-dev libsasl2-dev libgsasl-dev libxml2-dev libprotobuf-dev protobuf-compiler \
    # General
    python3 && \
    rm -rf /var/lib/apt/lists/*

# ── Apache Arrow apt repository ──────────────────────────────────────────────
RUN wget -q https://apache.jfrog.io/artifactory/arrow/$(lsb_release -is | tr '[:upper:]' '[:lower:]')/apache-arrow-apt-source-latest-$(lsb_release -cs).deb \
      -O /tmp/arrow.deb && \
    apt-get install -y /tmp/arrow.deb && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        libarrow-dev=${ARROW_VERSION}-1 \
        libparquet-dev=${ARROW_VERSION}-1 && \
    rm -rf /var/lib/apt/lists/* /tmp/arrow.deb

# ── PostgreSQL from source ───────────────────────────────────────────────────
RUN curl -fsSL "https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.gz" \
      -o /tmp/pg.tar.gz && \
    tar -xf /tmp/pg.tar.gz -C /tmp && \
    cd /tmp/postgresql-${PG_VERSION} && \
    ./configure --prefix=/usr/local/pgsql --with-openssl --with-readline --with-zlib --with-libxml && \
    make -j"$(nproc)" && \
    make install && \
    rm -rf /tmp/pg.tar.gz /tmp/postgresql-${PG_VERSION}

# ── AWS SDK for C++ ──────────────────────────────────────────────────────────
# Tarball downloads skip git submodules; clone with --recurse-submodules so
# aws-crt-cpp and its own deps are present for CMake.
RUN git clone --depth=1 --recurse-submodules --shallow-submodules \
      --branch ${AWS_SDK_VERSION} \
      https://github.com/aws/aws-sdk-cpp.git /tmp/aws-sdk-cpp && \
    cmake -S /tmp/aws-sdk-cpp -B /tmp/aws-build \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=${AWS_SDK_ROOT} \
        -DBUILD_ONLY="s3;core" \
        -DENABLE_TESTING=OFF \
        -DBUILD_SHARED_LIBS=ON \
        -DAUTORUN_UNIT_TESTS=OFF && \
    cmake --build /tmp/aws-build --parallel "$(nproc)" && \
    cmake --install /tmp/aws-build && \
    rm -rf /tmp/aws-sdk-cpp /tmp/aws-build

# ── libhdfs3 ─────────────────────────────────────────────────────────────────
# Patches needed for Ubuntu 24.04 + GCC 13:
#   1. Platform.cmake: -dumpversion → -dumpfullversion (GCC 13 returns bare "13")
#   2. GoogleTest REQUIRED → optional in CMakeLists (drop REQUIRED keyword)
#   3. test subdirectories removed (we only need the library, not its test binaries)
#   4. Build bundled googletest first — libhdfs3's main library links mock symbols
#      from gtest/gmock so the shared/static lib targets need real .a files.
RUN git clone --depth=1 https://github.com/apache/hawq /tmp/hawq && \
    sed -i 's/-dumpversion/-dumpfullversion/' \
        /tmp/hawq/depends/libhdfs3/CMake/Platform.cmake && \
    sed -i 's/FIND_PACKAGE(GoogleTest REQUIRED)/FIND_PACKAGE(GoogleTest)/' \
        /tmp/hawq/depends/libhdfs3/CMakeLists.txt && \
    sed -i '/[Aa][Dd][Dd]_\?[Ss][Uu][Bb][Dd][Ii][Rr][Ee][Cc][Tt][Oo][Rr][Yy].*test/Id' \
        /tmp/hawq/depends/libhdfs3/CMakeLists.txt && \
    cmake -S /tmp/hawq/depends/thirdparty/googletest \
          -B /tmp/gtest-build \
          -G Ninja \
          -DCMAKE_BUILD_TYPE=Release -DBUILD_GMOCK=ON \
          -DCMAKE_INSTALL_PREFIX=/usr/local && \
    cmake --build /tmp/gtest-build && \
    cmake --install /tmp/gtest-build && \
    rm -rf /tmp/gtest-build && \
    cmake -S /tmp/hawq/depends/libhdfs3 -B /tmp/hdfs3-build \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_CXX_FLAGS="-include cstdint -include sys/types.h" \
        -DGtest_LIBRARY=/usr/local/lib/libgtest.a \
        -DGmock_LIBRARY=/usr/local/lib/libgmock.a \
        -DGtest_INCLUDE_DIR=/usr/local/include \
        -DGmock_INCLUDE_DIR=/usr/local/include && \
    cmake --build /tmp/hdfs3-build --parallel "$(nproc)" && \
    cmake --install /tmp/hdfs3-build && \
    rm -rf /tmp/hawq /tmp/hdfs3-build

# ── ldconfig ─────────────────────────────────────────────────────────────────
RUN ldconfig

WORKDIR /src
