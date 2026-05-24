FROM apache/spark:4.1.1

USER root

# Install pip, curl and git (minimal apache/spark image ships none of these)
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends python3-pip curl git && \
    rm -rf /var/lib/apt/lists/*

# Required to pip-install into the system Python inside a container
ENV PIP_BREAK_SYSTEM_PACKAGES=1

RUN pip3 install --no-cache-dir \
    "fastavro>=1.9" \
    "pyarrow>=16.0" \
    "minio>=7.2" \
    "python-dotenv==1.2.2" \
    "pandas>=2.2.0" \
    "numpy>=1.26" \
    "vnstock>=3.0"

# Pre-bake S3A and Avro JARs into Spark's classpath — no internet access needed at job runtime
# hadoop-aws must match Spark 4.1.1's bundled Hadoop (3.4.1); mismatched versions cause
# NumberFormatException on duration strings like "60s" introduced in Hadoop 3.4.x
RUN curl -fL -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar" \
    && curl -fL -o /opt/spark/jars/aws-sdk-bundle-2.24.6.jar \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar" \
    && curl -fL -o /opt/spark/jars/spark-avro_2.13-4.1.1.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/4.1.1/spark-avro_2.13-4.1.1.jar"

RUN mkdir -p /tmp/spark-events && chmod 1777 /tmp/spark-events

# Bake mekong-jobs source into the image at /opt/project.
# In local dev, docker-compose.override.yml mounts the live checkout over this.
RUN git clone --depth=1 https://github.com/davidhoang2406/mekong-jobs.git /opt/project && \
    chown -R spark:spark /opt/project

USER spark
