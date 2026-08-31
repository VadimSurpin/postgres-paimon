FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive

# ── Arrow C++ from Apache apt repo ───────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg apt-transport-https && \
    curl -fsSL https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-noble.deb \
      -o /tmp/arrow.deb && \
    dpkg -i /tmp/arrow.deb && rm /tmp/arrow.deb && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      libarrow-dev libparquet-dev && \
    rm -rf /var/lib/apt/lists/*

# ── PostgreSQL 18 + runtime/build deps ───────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    gosu \
    libkrb5-dev libssl-dev \
    g++ make && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] \
      https://apt.postgresql.org/pub/repos/apt noble-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends postgresql-18 postgresql-server-dev-18 && \
    rm -rf /var/lib/apt/lists/*

# ── Build extension from source (stub out HDFS; demo uses S3/MinIO only) ─────
COPY extension /build/extension

# Replace AWS-SDK-based S3 offload with Arrow S3FileSystem (no extra headers needed)
COPY docker/demo/postgres/paimon_offload_s3_arrow.cpp /build/extension/paimon_offload_s3.cpp

# Replace paimon_offload_hdfs.cpp with a no-op stub so libhdfs3 is not needed
RUN printf '%s\n' \
    '// stub — HDFS not used in demo (S3/MinIO path is always taken)' \
    '#include "paimon_offload_hdfs.h"' \
    'namespace paimon {' \
    'bool hdfs_upload_table(const HdfsConfig&, const std::string&, const std::string&) { return false; }' \
    '} // namespace paimon' \
    > /build/extension/paimon_offload_hdfs.cpp

RUN cd /build/extension && \
    make \
      SHLIB_LINK="-lstdc++ -larrow -lparquet" \
      ARROW_INC=/usr/include/arrow \
      PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config \
      2>&1 | tail -5 && \
    cp paimon_heap.so /usr/lib/postgresql/18/lib/paimon_heap.so && \
    cp paimon_heap.control paimon_heap--1.0.sql /usr/share/postgresql/18/extension/

# Drop default cluster so the entrypoint init block runs fresh
RUN pg_dropcluster 18 main

COPY docker/demo/postgres/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5432
ENTRYPOINT ["/entrypoint.sh"]
