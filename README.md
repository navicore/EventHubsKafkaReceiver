[![Build Status](https://travis-ci.org/navicore/kafkatools.svg?branch=master)](https://travis-ci.org/navicore/kafkatools)

# KafkaTools

* a util to dump a kafka topic to a log

* publicly available docker image at [navicore/kafkatools](https://hub.docker.com/r/navicore/kafkatools) is the easiest way to use it.

commands:

```
docker run -it navicore/kafkatools --help
```

```
docker run -it navicore/kafkatools --broker-list broker-0:3456,broker-1:4567 --input-topic my-topic
```

* check sbt file for version info.

## experimenting with Concourse CI
```
fly -t onextent set-pipeline --pipeline kafkatools --config pipeline/pipeline.yml --var "private-key=$(cat ~/some_ci_id_rsa)" --var "docker-username=navicore" --var "docker-password=XXXXXXX" --var "github-access-token=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

