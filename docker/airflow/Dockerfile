# Airflow 2.11 (last 2.x line) + Java 17 + an isolated venv for Spark/Delta/GX jobs.
# The base image is pinned by digest; Python deps are installed from hashed lockfiles.
FROM apache/airflow:2.11.2-python3.11@sha256:aac8468234dfa802c5ac4d6318bbf3b620793b55569ac230f584dc3c1f0ca49d

ARG DELTA_VERSION=3.3.0
ARG DELTA_SPARK_SHA256=c6e93686b24e067d9645aa5f04b06fdfa938f18336f713b7f138994f9890ec9e
ARG DELTA_STORAGE_SHA256=951ce7e89468d1657aaf27067a5b9163a4bcccae3dd7d23d711f50bcd3365bb5

USER root
# The JRE comes from Debian's archive at build time. apt versions aren't pinned because
# Debian drops superseded versions from mirrors, so a pin would break fresh builds;
# the installed version is printed during build for traceability.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
    && java -version
ENV JAVA_HOME=/opt/java

COPY requirements/jobs.lock.txt requirements/dev.lock.txt /tmp/requirements/
RUN python -m venv /opt/venv/jobs \
    && /opt/venv/jobs/bin/pip install --no-cache-dir --require-hashes -r /tmp/requirements/jobs.lock.txt \
    && /opt/venv/jobs/bin/pip install --no-cache-dir --require-hashes -r /tmp/requirements/dev.lock.txt \
    && SPARK_JARS=/opt/venv/jobs/lib/python3.11/site-packages/pyspark/jars \
    && curl -fsSL -o "$SPARK_JARS/delta-spark_2.12-${DELTA_VERSION}.jar" \
        "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
    && curl -fsSL -o "$SPARK_JARS/delta-storage-${DELTA_VERSION}.jar" \
        "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    && echo "${DELTA_SPARK_SHA256}  $SPARK_JARS/delta-spark_2.12-${DELTA_VERSION}.jar" | sha256sum -c - \
    && echo "${DELTA_STORAGE_SHA256}  $SPARK_JARS/delta-storage-${DELTA_VERSION}.jar" | sha256sum -c - \
    && chmod -R a+rX /opt/venv

USER airflow
# pytest for DAG-integrity tests and confluent-kafka for the task callbacks (Phase 2),
# both pinned to the versions in Airflow's own constraints.
RUN pip install --no-cache-dir "pytest==8.2.1" "confluent-kafka==2.13.2" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.11.txt"

COPY --chown=airflow:root pipelines /opt/sentinel/pipelines
COPY --chown=airflow:root src /opt/sentinel/src
COPY --chown=airflow:root scripts /opt/sentinel/scripts
COPY --chown=airflow:root config /opt/sentinel/config

ENV PYTHONPATH=/opt/sentinel:/opt/sentinel/src \
    SENTINEL_JOBS_PYTHON=/opt/venv/jobs/bin/python \
    PYSPARK_PYTHON=/opt/venv/jobs/bin/python \
    TZ=UTC
WORKDIR /opt/sentinel
