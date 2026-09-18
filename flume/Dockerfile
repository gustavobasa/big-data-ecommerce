FROM eclipse-temurin:8-jre
RUN apt-get update \
    && apt-get install -y curl tar \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt
RUN curl -L https://archive.apache.org/dist/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz \
    -o apache-flume.tar.gz \
    && tar -xzf apache-flume.tar.gz \
    && mv apache-flume-1.11.0-bin flume \
    && rm apache-flume.tar.gz
RUN mkdir -p /data/incoming \
    /data/output \
    /data/state \
    /opt/flume/conf
COPY flume-teste.conf /opt/flume/conf/flume-teste.conf
CMD ["/opt/flume/bin/flume-ng", "agent", "--conf", "/opt/flume/conf", "--conf-file", "/opt/flume/conf/flume-teste.conf", "--name", "a1", "-Dflume.root.logger=INFO,console"]