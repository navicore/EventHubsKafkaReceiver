# UNDER CONSTRUCTION - NOT WORKING QUITE YET

# EventHubs Kafka Receiver

write data from an Azure Event Hub to Kafka

## REQUIRED ENV VARS

```shell
    EH_PROGESSS_DIR=<CHANGE ME>
    EH_POLICY_NAME=<CHANGE ME>
    EH_POLICY_KEY=<CHANGE ME>
    EH_NAMESPACE=<CHANGE ME>
    EH_HUB_NAME=<CHANGE ME>
    KAFKA_TOPIC=<CHANGE ME>
```

## OPTIONAL ENV VARS
```shell
    EH_BATCH_DUR="2"
    EH_PARTITIONS="4"
    EH_CONSUMER_GROUP="$Default"
    KAFKA_BROKER_LIST="localhost:9092"
```

## Build Jar

```shell
sbt assembly
```

## Build Docker Image

```shell
sbt assembly && docker build -t myimage .
```

## Deploy via Kubernetes

TODO

TODO

TODO

