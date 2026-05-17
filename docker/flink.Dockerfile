# Stage 1 — source of JDK development headers (jni.h etc.)
# flink:1.20-java17 ships a JRE only; PyFlink's build script requires the
# JDK include directory to exist at $JAVA_HOME/include before it will install.
FROM eclipse-temurin:17-jdk AS jdk-headers

# Stage 2 — final image: Flink + Python + PyFlink + Kafka connector
FROM flink:2.0-java17

USER root

COPY --from=jdk-headers /opt/java/openjdk/include /opt/java/openjdk/include

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# Allow pip to install into the system Python (intentional in a container)
ENV PIP_BREAK_SYSTEM_PACKAGES=1

RUN ln -s /usr/bin/python3 /usr/bin/python

# PyFlink version must match the Flink cluster version
RUN pip3 install apache-flink==2.0.0 python-dotenv

# Kafka connector JAR pre-loaded into Flink's lib dir — available to all
# submitted jobs automatically, no env.add_jars() needed in job code.
RUN curl -fL -o $FLINK_HOME/lib/flink-sql-connector-kafka-4.0.1-2.0.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar"
