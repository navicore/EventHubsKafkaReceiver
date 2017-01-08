FROM navicore/docker-java-sbt

# install kafka for the command line tools
RUN wget -q http://mirrors.koehn.com/apache/kafka/0.10.0.1/kafka_2.11-0.10.0.1.tgz && tar xzf kafka* 

# install spark for when running in local[*] mode
RUN wget -q http://d3kbcqa49mib13.cloudfront.net/spark-1.6.1-bin-hadoop2.6.tgz && tar xzf spark* 

ADD . /app

RUN cd /app && \
    sbt assembly && \
    cp target/scala-2.11/onextent-KafkaTools.jar /app/ && \
    sbt clean

WORKDIR /app

ENTRYPOINT ["java", "-jar", "/app/onextent-KafkaTools.jar"]

