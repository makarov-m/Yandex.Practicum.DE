# The 9th Project

### DWH requirements
The purpose of building the DWH. Business requirements:
Architech plans to launch user tagging in the application based on order statistics. For example, a user has ordered pizza 10 times—we assign him the tag “Pizza Lover.”

How we count orders:
- We carry out all calculations only for closed orders with the CLOSED status.

Functional requirements:
- Input data format - JSON

Features of layers:
- In STG - initial data as is.
- In DDS - Data Vault data model.
- In CDM there are two showcases:
	- The first display case is a counter for orders by dishes;
	- The second is a counter of orders by product category.

Non-functional requirements:
The first channel is the flow of orders that goes to Kafka (5 orders per minute).
The second channel is dictionary data (dishes, restaurants, users) that goes to Redis.

PostgreSQL is used as the database. The data processing logic needs to be written in Python; it will be deployed in Kubernetes. The message broker for both input and data exchange between services is Kafka. It is necessary to ensure idempotency in processing messages from the broker.

### Action Plan
1. First you need to raise the services that receive input data. This way you can immediately study the original information. Let's expand - Redis and Kafka, the order does not matter.
2. It is worth continuing to roll out the infrastructure so that all parts of DWH are ready for development. The systems for obtaining data are up, which means that the next step is to launch the database for storage.
3. In order to deploy a microservice architecture in the cloud and all users have access to the created images, it is necessary to additionally raise the Container Registry, which will later be required to launch the written services.
4. When all the tools are deployed, you can move on to developing the DWH.
5. The STG layer is the first in the order of data processing. This means that you first need to write a service that fills this layer - an STG service.
6. After the STG layer comes the DDS layer, so after the STG service we implement the DDS service.
7. The last stage is to build showcases. Therefore, it is necessary to implement a CDM service that will calculate storefronts.
8. Datalens is a service for business analytics from Yandex. The tool is provided free of charge.

### Workflow schema

![](/pics/schema.png)

### Service Structure

```
- app
    - Chart.yaml
    - values.yaml
- src
    - lib
        - kafka_connect
        - redis
        - pg
    - layer_loader
        - repository 
            - layer_loader
        - layer_messgae_processor.py
    - app.py
    - app_config.py
- dockerfile
- requirements.txt 
```

The app directory contains a Helm Chart - a set of instructions on exactly how to deploy your service in Kubernetes using the Helm utility.

The src directory contains the source code of our service in the layer_loader subdirectory, and the lib subdirectory contains code for connecting to Kafka, Redis and Postgres.

The app.py file defines the structure of your service;
The connection configuration is set in app_config.py;
In the Dockerfile we describe the logic for building the container image;
The requirement.txt file contains a list of required libraries.

## REDIS init

```bash
# Check: post to API redis Yandex Cloud to test if the DB is ready
curl -X POST https://redis-data-service.sprint9.tgcloudenv.ru/test_redis \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "redis":{
        "host": "c-c9qrmej16t8lg8oc3blu.rw.mdb.yandexcloud.net",
        "port": 6380,
        "password": "<...>"
    }
}
EOF
```

```bash
#### Load: USERS table
curl -X POST https://redis-data-service.sprint9.tgcloudenv.ru/load_users \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "redis":{
        "host": "c-c9qrmej16t8lg8oc3blu.rw.mdb.yandexcloud.net",
        "port": 6380,
        "password": "<...>"
    }
}
EOF
```

```bash
# Load: RESTAURANTS table
curl -X POST https://redis-data-service.sprint9.tgcloudenv.ru/load_restaurants \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "redis":{
        "host": "c-c9qrmej16t8lg8oc3blu.rw.mdb.yandexcloud.net",
        "port": 6380,
        "password": "<...>"
    }
}
EOF
```

## KAFKA

```bash
# load metadata
docker run \
    -it \
    --network=host \
    -v "/Users/max/Documents/practicum/sprint-9/CA.pem:/data/CA.pem" \
    edenhill/kcat:1.7.1 \
    -b rc1a-1707194m2p4hsm7d.mdb.yandexcloud.net:9091 \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanisms=SCRAM-SHA-512 \
    -X sasl.username=producer_consumer \
    -X sasl.password="<...>" \
    -X ssl.ca.location=/data/CA.pem \
    -L
```

```bash
# Read data: consumer
docker run \
    -it \
    --name "kcat" \
    --network=host \
    --rm \
    -v "/Users/max/Documents/practicum/sprint-9/CA.pem:/data/CA.pem" \
    edenhill/kcat:1.7.1 -b rc1a-1707194m2p4hsm7d.mdb.yandexcloud.net:9091 \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanisms=SCRAM-SHA-512 \
    -X sasl.username=producer_consumer \
    -X sasl.password="<...>" \
    -X ssl.ca.location=/data/CA.pem \
    -t order-service_orders \
    -C \
    -o beginning 
```

```bash
# test kafka
curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/test_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data '{
    "student": "smartflip",
    "kafka_connect":{
        "host": "rc1a-1707194m2p4hsm7d.mdb.yandexcloud.net",
        "port": 9091,
        "topic": "order-service_orders",
        "producer_name": "producer_consumer",
        "producer_password": "<...>"
    }
}'
```

```bash
# register kafka
curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/register_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data '{
    "student": "smartflip",
    "kafka_connect":{
        "host": "rc1a-1707194m2p4hsm7d.mdb.yandexcloud.net",
        "port": 9091,
        "topic": "order-service_orders",
        "producer_name": "producer_consumer",
        "producer_password": "<...>"
    }
}'
```

## Cloud Postgres

```bash
# send post request to cloud postgres
curl -X POST https://postgres-check-service.sprint9.tgcloudenv.ru/init_schemas \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
  "student": "smartflip",
  "pg_settings": {
    "host": "rc1b-kn77wjd3ug2zcg3o.mdb.yandexcloud.net",
    "port": 6432,
    "dbname": "sprint9dwh",
    "username": "de_max",
    "password": "<...>"
  }
}
EOF
```

### K8s / Helm

```bash
# running helm deployments
helm ls -a -n c08-maksim-makarov
```

```bash
# uninstall deployemnt 
helm delete stg-service -n c08-maksim-makarov
helm delete dds-service -n c08-maksim-makarov
helm delete cdm-service -n c08-maksim-makarov
```

```bash
# Prepare kubeconfig: write the path to the kubeconfig file to a variable
export KUBECONFIG=/Users/max/.kube/config
```

```bash
# STG
cd /Users/max/Documents/GitHub/de-project-sprint-9/solution/service_stg
docker build . -t cr.yandex/crp4l665spjbrg60ja0c/stg_service:v2023-05-06-r1 --platform linux/amd64
docker push cr.yandex/crp4l665spjbrg60ja0c/stg_service:v2023-05-06-r1
# give acess for pulling images
# change tag in helm
cd /Users/max/Documents/GitHub/de-project-sprint-9/solution/service_stg
helm install stg-service app -n c08-maksim-makarov
kubectl get pods
```

```bash
# DDS
cd /Users/max/Documents/GitHub/de-project-sprint-9/solution/service_dds
docker build . -t cr.yandex/crp4l665spjbrg60ja0c/dds_service:v2023-05-06-r1 --platform linux/amd64
docker push cr.yandex/crp4l665spjbrg60ja0c/dds_service:v2023-05-06-r1
# give acess for pulling images
# change tag in helm
cd /Users/max/Documents/GitHub/de-project-sprint-9/solution/service_dds
helm install dds-service app -n c08-maksim-makarov
kubectl get pods
```


```bash
# CDM
cd /Users/max/Documents/GitHub/de-project-sprint-9/solution/service_cdm
docker build . -t cr.yandex/crp4l665spjbrg60ja0c/cdm_service:v2023-05-06-r1 --platform linux/amd64
docker push cr.yandex/crp4l665spjbrg60ja0c/cdm_service:v2023-05-06-r1
# give acess for pulling images
# change tag in helm
cd /Users/max/Documents/GitHub/de-project-sprint-9/solution/service_cdm
helm install cdm-service app -n c08-maksim-makarov
kubectl get pods
```
