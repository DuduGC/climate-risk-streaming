FROM flink:1.20.3-scala_2.12-java17 AS python-builder

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        libbz2-dev \
        libffi-dev \
        libgdbm-dev \
        liblzma-dev \
        libreadline-dev \
        libsqlite3-dev \
        libssl-dev \
        uuid-dev \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY docker/flink/checksums.sha256 /tmp/checksums.sha256

RUN curl --fail --location --retry 5 --retry-delay 2 --connect-timeout 10 --max-time 300 \
        https://www.python.org/ftp/python/3.11.14/Python-3.11.14.tar.xz \
        --output /tmp/Python-3.11.14.tar.xz \
    && curl --fail --location --retry 5 --retry-delay 2 --connect-timeout 10 --max-time 180 \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
        --output /tmp/flink-sql-connector-kafka-3.3.0-1.20.jar \
    && cd /tmp \
    && sha256sum --check checksums.sha256 \
    && tar -xJf /tmp/Python-3.11.14.tar.xz -C /tmp \
    && cd /tmp/Python-3.11.14 \
    && ./configure --prefix=/opt/python3.11 --enable-shared --with-ensurepip=install \
    && make -j2 \
    && make altinstall \
    && printf '%s\n' /opt/python3.11/lib > /etc/ld.so.conf.d/python3.11.conf \
    && ldconfig \
    && /opt/python3.11/bin/python3.11 --version \
    && rm -rf /tmp/Python-3.11.14 /tmp/Python-3.11.14.tar.xz

COPY requirements-flink.txt /tmp/requirements-flink.txt

RUN /opt/python3.11/bin/python3.11 -m pip install \
        --disable-pip-version-check \
        --no-cache-dir \
        --prefix=/opt/python3.11 \
        -r /tmp/requirements-flink.txt \
    && rm /tmp/requirements-flink.txt

FROM flink:1.20.3-scala_2.12-java17 AS runtime

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libbz2-1.0 \
        libffi8 \
        libgdbm6 \
        liblzma5 \
        libreadline8 \
        libsqlite3-0 \
        libssl3 \
        libuuid1 \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=python-builder /opt/python3.11 /opt/python3.11
COPY --from=python-builder /tmp/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar

RUN printf '%s\n' /opt/python3.11/lib > /etc/ld.so.conf.d/python3.11.conf \
    && ldconfig \
    && /opt/python3.11/bin/python3.11 -c "import bz2, ctypes, lzma, sqlite3, ssl, zlib"

COPY --chown=flink:flink src /opt/climate/src

ENV PATH="/opt/python3.11/bin:${PATH}" \
    PYTHONPATH=/opt/climate/src \
    PYTHONUNBUFFERED=1

USER flink