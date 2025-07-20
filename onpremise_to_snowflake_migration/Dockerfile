# Dockerfile
FROM debezium/connect:1.4

# Install S3 connector
USER root


COPY confluentinc-kafka-connect-s3-10.6.7.zip /kafka/connect/

RUN cd /kafka/connect && \
    jar -xf confluentinc-kafka-connect-s3-10.6.7.zip && \
    rm confluentinc-kafka-connect-s3-10.6.7.zip



USER kafka
