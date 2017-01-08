package onextent.eventhubs.receiver

import java.util.Properties

import com.github.benfradet.spark.kafka010.writer._
import org.apache.kafka.common.serialization.{BytesSerializer, StringSerializer}
import com.microsoft.azure.eventhubs.EventData
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.EventHubsUtils

object Main extends Serializable with LazyLogging {

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load().getConfig("main")

    val progressDir = config.getString("eventhubs.progressDir")
    val batchDuration = config.getString("eventhubs.batchDuration").toInt
    val namespace = config.getString("eventhubs.eventHubNamespace")
    val hubname = config.getString("eventhubs.eventHubName")

    val eventhubParameters = Map[String, String] (
      "eventhubs.policyname" -> config.getString("eventhubs.policyName"),
      "eventhubs.policykey" -> config.getString("eventhubs.policyKey"),
      "eventhubs.namespace" -> namespace,
      "eventhubs.name" -> hubname,
      "eventhubs.partition.count" -> config.getString("eventhubs.partitions"),
      "eventhubs.consumergroup" -> config.getString("eventhubs.consumerGroup")
    )

    val sparkConfig = new SparkConf().set("spark.cores.max", "3").set("spark.cores.min", "3")
    val ssc = new StreamingContext(new SparkContext(sparkConfig), Seconds(batchDuration))

    logger.info(s"starting receiver for $namespace on $hubname")
    val ehSteam: DStream[EventData] = EventHubsUtils.createDirectStreams(
      ssc,
      namespace,
      progressDir,
      Map(hubname -> eventhubParameters))

    val producerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", config.getString("kafka.brokerList"))
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      //p.setProperty("value.serializer", classOf[BytesSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }

    val topic = config.getString("kafka.topic")
    //ehSteam.writeToKafka[String, Array[Byte]](
    //  producerConfig,
    //  ehData => new ProducerRecord[String, Array[Byte]](topic, ehData.getBody)
    //)
    ehSteam.writeToKafka[String, String](
      producerConfig,
      ehData => new ProducerRecord[String, String](topic, new String(ehData.getBody))
    )
    ssc.start()
    ssc.awaitTermination()
  }
}

