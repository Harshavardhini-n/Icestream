FROM flink:1.18.1-scala_2.12-java11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir apache-flink==1.18.1

USER flink