/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.eventhubs

import java.util.concurrent.ConcurrentHashMap

import scala.collection.{mutable, Map}

import com.microsoft.azure.eventhubs._

import org.apache.spark.SparkEnv
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener}

/**
 * ReliableEventHubsReceiver offers the ability to reliably store data into BlockManager without
 * loss.
 * It is turned off by default and will be enabled when
 * spark.streaming.receiver.writeAheadLog.enable is true.
 *
 * The difference compared to EventHubsReceiver is that the offset is updated in persistent
 * store only after data is reliably stored as write-ahead log, so the potential data loss
 * problem of EventHubsReceiver can be eliminated.
 */
private[eventhubs]
class ReliableEventHubsReceiver(
    eventhubsParams: Map[String, String],
    partitionId: String,
    storageLevel: StorageLevel,
    offsetStore: Option[OffsetStore],
    receiverClient: EventHubsClientWrapper,
    maximumEventRate: Int)
  extends EventHubsReceiver(
    eventhubsParams, partitionId, storageLevel, offsetStore, receiverClient, maximumEventRate) {

  override def onStop() {
    super.onStop()
    if (blockGenerator != null) {
      blockGenerator.stop()
      blockGenerator = null
    }
    if (blockOffsetMap != null) {
      blockOffsetMap.clear()
      blockOffsetMap = null
    }
  }

  override def onStart() {
    blockOffsetMap = new ConcurrentHashMap[StreamBlockId, String]
    // Initialize the block generator for storing EventHubs message.
    blockGenerator = new BlockGenerator(new GeneratedBlockHandler, streamId, SparkEnv.get.conf)
    blockGenerator.start()
    super.onStart()
  }

  @deprecated
  override def processReceivedMessage(eventData: EventData): Unit = {
    blockGenerator.addDataWithCallback(eventData.getBody, eventData.getSystemProperties.getOffset)
  }

  override def processReceivedMessagesInBatch(eventDataBatch: Iterable[EventData]): Unit = {
    val maximumSequenceNumber = eventDataBatch.map(x => x.getSystemProperties.getSequenceNumber).
      reduceLeft { (x, y) => if (x > y) x else y }
    val offsetMetadata = eventDataBatch.find(x =>
      x.getSystemProperties.getSequenceNumber == maximumSequenceNumber).get.getSystemProperties.
      getOffset
    /**
     * It is guaranteed by Eventhubs that the event data with the highest sequence number has the
     * largest offset
     */
    blockGenerator.addMultipleDataWithCallback(eventDataBatch.map(x => x.getBody).toIterator,
      offsetMetadata)
  }

  /**
   * Store the ready-to-be-stored block and commit the related offsets to OffsetStore. This method
   * will try a fixed number of times to push the block. If the push fails, the receiver is stopped.
   */
  private def storeBlockAndCommitOffset(
      blockId: StreamBlockId,
      arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
    var count = 0
    var pushed = false
    var exception: Exception = null
    while (!pushed && count < RETRY_COUNT) {
      try {
        store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[Array[Byte]]])
        pushed = true
      } catch {
        case e: Exception =>
          count += 1
          exception = e
          Thread.sleep(SECONDS_BETWEEN_RETRY * 1000)
      }
    }
    if (pushed) {
      // commit the latest offset of the block to offsetToSave, when the checkpoint interval
      // passes the offset is saved to offset store
      offsetToSave = blockOffsetMap.get(blockId)
      blockOffsetMap.remove(blockId)
    } else {
      stop("Error while storing block into Spark", exception)
    }
  }

  /** Class to handle blocks generated by the block generator. */
  private final class GeneratedBlockHandler extends BlockGeneratorListener {

    def onAddData(data: Any, metadata: Any): Unit = {
      // Update the offset of the data that was added to the generator
      if (metadata != null) {
        val offset = metadata.asInstanceOf[String]
        latestOffsetCurBlock = offset
      }
    }

    def onGenerateBlock(blockId: StreamBlockId): Unit = {

      // Remember the offsets when a block has been generated
      blockOffsetMap.put(blockId, latestOffsetCurBlock)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {

      // Store block and commit the blocks offset
      storeBlockAndCommitOffset(blockId, arrayBuffer)
    }

    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }

  /** Use block generator to generate blocks to Spark block manager synchronously */
  private var blockGenerator: BlockGenerator = _

  /** A string to store the latest offset in the current block for the current partition. */
  private var latestOffsetCurBlock: String = _

  /** A concurrent HashMap to store the stream block id and related offset snapshot. */
  private var blockOffsetMap: ConcurrentHashMap[StreamBlockId, String] = _

  private val RETRY_COUNT: Int = 10

  private val SECONDS_BETWEEN_RETRY = 1
}
