# UNDER CONSTRUCTION - NOT WORKING QUITE YET

# temporarily inlining the src from the azure event hubs master repo (waiting on a jar for v2.0.3)

# EventHubs Kafka Receiver

using Spark v2.0.2 to write data to Kafka v0.10.x from an Azure Event Hub

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

