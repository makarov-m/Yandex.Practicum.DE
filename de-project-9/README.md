# Проект 9-го спринта

### Требования к DWH
Цель построения ДВХ, бизнес требования:
Architech планирует запустить тегирование пользователей в приложении на основе статистики по заказам. Например, пользователь заказал 10 раз пиццу — присваиваем ему тег «Любитель пиццы». 

Как считаем заказы: 
- Все расчёты ведём только по закрытым заказам со статусом CLOSED.

Функциональные требования:
- Формат входных данных - JSON

Особенности слоёв: 
- В STG — исходные данные as is.
- В DDS — модель данных Data Vault.
- В CDM — две витрины:
	-	Первая витрина — счётчик заказов по блюдам; 
	-	Вторая — счётчик заказов по категориям товаров.

Нефункциональные требования:
Первый канал — это поток заказов, который идёт в Kafka (5 заказов в минуту).
Второй канал — это словарные данные (блюда, рестораны, пользователи), которые идут в Redis.

В качестве БД используется PostgreSQL. Логику обработки данных нужно написать на Python, она будет разворачиваться в Kubernetes. Брокер сообщений как на вход, так и для обмена данными между сервисами — Kafka. Надо обеспечить идемпотентность обработки сообщений из брокера.

### Action Plan
1. Сначала нужно поднять сервисы, в которые поступают входные данные. Так можно сразу изучить оригинальную информацию. Рзворачиваем — Redis и Kafka, порядок не имеет значения.
2. Стоит продолжить разворачивать инфраструктуру, чтобы все части DWH были готовы к разработке. Системы для получения данных подняты, значит, на очереди запуск базы под хранилище. 
3. Чтобы развернуть микросервисную архитектуру в облаке и все пользователи имели доступ к созданным образам необходимо дополнительно поднять Container Registry, который в дальнейшем потребуется для запуска написанных сервисов. 
4. Когда все инструменты развернуты, можно переходить за разработку DWH.
5. Слой STG — первый по порядку обработки данных. Значит, сначала нужно написать сервис, который заполняет этот слой, — STG-сервис.
6. После слоя STG идёт слой DDS, поэтому следом за STG-сервисом реализуем DDS-сервис.
7. Последний этап — построить витрины. Поэтому необходимо реализовать CDM-сервис, который рассчитает витрины.
8. Datalens — это сервис для бизнес-аналитики от компании Яндекс. Инструмент предоставляется бесплатно.

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

В каталоге app лежит Helm Chart — набор инструкций, как именно развернуть ваш сервис в Kubernetes c помощью утилиты Helm. 

В каталоге src находится исходный код нашего сервиса в подкаталоге layer_loader, а также подкатолог lib находится код для подключения к Kafka, Redis и Postgres.

Файл app.py задаёт структуру вашего сервиса;
Конфигурация подключения задается в app_config.py; 
В Dockerfile описываем логику сборки образа контейнера; 
В файле requirement.txt содержится список необходимых библиотек. 

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
        "password": "adminadmin"
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
        "password": "adminadmin"
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
        "password": "adminadmin"
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
    -X sasl.password="adminadmin" \
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
    -X sasl.password="adminadmin" \
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
        "producer_password": "adminadmin"
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
        "producer_password": "adminadmin"
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
    "password": "adminadmin"
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
# Подготовьте kubeconfig: запишите путь до файла kubeconfig в переменную
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

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-9` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-9.git`
3. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-9`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GitHub-аккаунте:
	* `git push origin main`
