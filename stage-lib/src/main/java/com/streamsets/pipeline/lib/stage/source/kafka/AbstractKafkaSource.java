/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractKafkaSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractKafkaSource.class);
  private static final String CLIENT_PREFIX = "StreamSetsKafkaConsumer";
  private static final String DOT = ".";

  /****************** Start config options *******************/

  @ConfigDef(required = true,
    type = ConfigDef.Type.STRING,
    label = "Topic",
    defaultValue = "myTopic")
  public String topic;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    label = "Partition",
    defaultValue = "0")
  public int partition;

  @ConfigDef(required = true,
    type = ConfigDef.Type.STRING,
    description = "A known kafka broker. Does not have to be the leader of the partition",
    label = "Broker Host",
    defaultValue = "localhost")
  public String brokerHost;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    label = "Broker Port",
    defaultValue = "9092")
  public int brokerPort;

  @ConfigDef(required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "From Beginning",
    defaultValue = "false")
  public boolean fromBeginning;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    description = "The maximum data per batch. The source uses this size when making a fetch request from kafka",
    label = "Max Fetch Size",
    defaultValue = "64000")
  public int maxBatchSize;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    description = "The maximum wait time in seconds before the kafka fetch request returns if no message is available.",
    label = "Max Wait Time",
    defaultValue = "1000")
  public int maxWaitTime;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    description = "The minimum data per batch. The source uses this size when making a fetch request from kafka",
    label = "Min Fetch Size",
    defaultValue = "8000")
  public int minBatchSize;

  /****************** End config options *******************/

  private KafkaConsumer kafkaConsumer;

  @Override
  public void init() {
    kafkaConsumer = new KafkaConsumer(topic, partition, new KafkaBroker(brokerHost, brokerPort), minBatchSize,
      maxBatchSize, maxWaitTime, CLIENT_PREFIX + DOT + topic + DOT + partition);
    kafkaConsumer.init();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    long offsetToRead;
    if(lastSourceOffset == null || lastSourceOffset.isEmpty()) {
      offsetToRead = kafkaConsumer.getOffsetToRead(fromBeginning);
    } else {
      offsetToRead = Long.parseLong(lastSourceOffset);
    }

    List<MessageAndOffset> partitionToPayloadList = new ArrayList<>();
    try {
      partitionToPayloadList.addAll(kafkaConsumer.read(offsetToRead));
    } catch (SocketTimeoutException e) {
      //If the value of consumer.timeout.ms is set to a positive integer, a timeout exception is thrown to the
      //consumer if no message is available for consumption after the specified timeout value.
      //If this happens exit gracefully
      LOG.warn("SocketTimeoutException encountered while fetching message from Kafka.");
    } catch (StageException e) {
      throw e;
    } catch (Exception e) {
      throw new StageException(null, e.getMessage(), e);
    }

    String offsetToReturn = null;
    int recordCounter = 0;
    for(MessageAndOffset partitionToPayloadMap : partitionToPayloadList) {
      //create record by parsing the message payload based on the pay load type configuration
      //As of now handle just String
      if(recordCounter == maxBatchSize) {
        //even though kafka has many messages, we need to cap the number of records to a value indicated by maxBatchSize.
        //return the offset of the previous record so that the next time we get start from this message which did not
        //make it to this batch.
        break;
      }
      recordCounter++;
      Record record = getContext().createRecord(topic + "." + partition + "." + System.currentTimeMillis() + "."
        + recordCounter);
      ByteBuffer payload  = partitionToPayloadMap.getPayload();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);

      offsetToReturn = String.valueOf(partitionToPayloadMap.getOffset());
      populateRecordFromBytes(record, bytes);
      batchMaker.addRecord(record);
    }
    return offsetToReturn;
  }

  @Override
  public void destroy() {
    kafkaConsumer.destroy();
  }

  protected abstract void populateRecordFromBytes(Record record, byte[] bytes) throws StageException;

}
