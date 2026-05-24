FROM python:3.12-slim

USER root

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends curl git default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY docker/jupyter-requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt jupyterlab>=4.2

RUN SPARK_JARS=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    curl -fL -o $SPARK_JARS/hadoop-aws-3.4.1.jar \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar" && \
    curl -fL -o $SPARK_JARS/aws-sdk-bundle-2.24.6.jar \
        "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar" && \
    curl -fL -o $SPARK_JARS/spark-avro_2.13-4.1.1.jar \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/4.1.1/spark-avro_2.13-4.1.1.jar"

RUN useradd -m -u 1000 jovyan && \
    mkdir -p /tmp/spark-events && chmod 1777 /tmp/spark-events && \
    mkdir -p /opt/project && chown jovyan:jovyan /opt/project

WORKDIR /opt/project

ENV PYTHONPATH=/opt/project

USER jovyan

EXPOSE 8888

CMD ["sh", "-c", "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token=${JUPYTER_TOKEN:-} --ServerApp.password='' --notebook-dir=/opt/project/notebooks"]
