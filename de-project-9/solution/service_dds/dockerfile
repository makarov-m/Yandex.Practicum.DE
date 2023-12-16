FROM python:3.10

ARG KAFKA_HOST
ARG KAFKA_PORT
ARG KAFKA_CONSUMER_USERNAME
ARG KAFKA_CONSUMER_PASSWORD
ARG KAFKA_CONSUMER_GROUP
ARG KAFKA_SOURCE_TOPIC
ARG KAFKA_DESTINATION_TOPIC

ARG PG_WAREHOUSE_HOST
ARG PG_WAREHOUSE_PORT
ARG PG_WAREHOUSE_DBNAME
ARG PG_WAREHOUSE_USER
ARG PG_WAREHOUSE_PASSWORD

RUN apt-get update -y

COPY . .

RUN pip install -r requirements.txt

RUN mkdir -p /crt
RUN wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" --output-document /crt/YandexInternalRootCA.crt
RUN chmod 0600 /crt/YandexInternalRootCA.crt

WORKDIR /src
ENTRYPOINT ["python"]

CMD ["app.py"]