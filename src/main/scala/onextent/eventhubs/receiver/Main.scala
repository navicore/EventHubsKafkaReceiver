package onextent.eventhubs.receiver

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.EventHubsUtils

object Main extends Serializable with LazyLogging {

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load().getConfig("main")

    val progressDir = config.getString("eventhubs.progressDir")
    val policyName = config.getString("eventhubs.policyName")
    val policykey = config.getString("eventhubs.policyKey")
    val eventHubNamespace = config.getString("eventhubs.eventHubNamespace")
    val eventHubName = config.getString("eventhubs.eventHubName")
    val batchDuration = config.getString("eventhubs.batchDuration").toInt
    val partitions = config.getString("eventhubs.partitions")
    val consumerGroup = config.getString("eventhubs.consumerGroup")

    val eventhubParameters = Map[String, String] (
      "eventhubs.policyname" -> policyName,
      "eventhubs.policykey" -> policykey,
      "eventhubs.namespace" -> eventHubNamespace,
      "eventhubs.name" -> eventHubName,
      "eventhubs.partition.count" -> partitions,
      "eventhubs.consumergroup" -> consumerGroup
    )

    val ssc = new StreamingContext(new SparkContext(), Seconds(batchDuration))

    logger.info(s"starting receiver for $eventHubNamespace on $eventHubName")
    val inputDirectStream = EventHubsUtils.createDirectStreams(
      ssc,
      eventHubNamespace,
      progressDir,
      Map(eventHubName -> eventhubParameters))

    inputDirectStream.foreachRDD { rdd =>
      //rdd.flatMap(eventData => new String(eventData.getBody).split(" ").map(_.replaceAll(
      //  "[^A-Za-z0-9 ]", ""))).map(word => (word, 1)).reduceByKey(_ + _).collect().toList.
      //  foreach(println)
      rdd.foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

