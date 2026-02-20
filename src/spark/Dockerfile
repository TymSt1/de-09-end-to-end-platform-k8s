FROM apache/spark:3.5.4-python3

USER root

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Download Hadoop AWS and AWS SDK JARs for S3 access
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

COPY batch_process.py /app/

USER spark

ENTRYPOINT ["/opt/spark/bin/spark-submit", "/app/batch_process.py"]