# The 8th Project

### Description
The task is to pick up messages from Kafka, process and send them to receivers: a Postgres database and a new topic for the Kafka broker.

### Schema
![](src/pics/schema.png)

### Kcat commands

```
# read message from the topic OUT
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username="de-student" \
-X sasl.password="ltcneltyn" \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t project8.mmakarov_out  \
-K \
-C 
```

```
# send message to the IN topic
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username="de-student" \
-X sasl.password="ltcneltyn" \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t project8.mmakarov_in \
-K: \
-P 
key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}
```

### Check the local Postgres database

```
# bash command
psql -h localhost -p 5432 -d de -U jovyan
```

```
// SQL command
select * from subscribers_feedback;
```

### Repository structure

There are two sub-folders inside `src`:
- `/src/pics`;
- `/src/scripts`.
