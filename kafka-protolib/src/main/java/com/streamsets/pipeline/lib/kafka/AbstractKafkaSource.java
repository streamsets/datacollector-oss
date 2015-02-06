/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.util.KafkaStageLibError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@ConfigGroups(value = AbstractKafkaSource.KafkaSourceConfigGroups.class)
public abstract class AbstractKafkaSource extends BaseSource {

  public enum KafkaSourceConfigGroups implements Label {
    KAFKA_PROPERTIES("Kafka Configuration Properties");

    private final String label;

    private KafkaSourceConfigGroups(String label) {
      this.label = label;
    }

    public String getLabel() {
      return this.label;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractKafkaSource.class);
  private static final String CLIENT_PREFIX = "StreamSetsKafkaConsumer";
  private static final String DOT = ".";

  /****************** Start config options *******************/

  @ConfigDef(required = true,
    type = ConfigDef.Type.STRING,
    description = "The Kafka topic from which the messages must be read",
    label = "Topic",
    group = "KAFKA_PROPERTIES",
    defaultValue = "topicName")
  public String topic;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    description = "The partition of Kafka topic from which the messages must be read",
    label = "Partition",
    group = "KAFKA_PROPERTIES",
    defaultValue = "0")
  public int partition;

  @ConfigDef(required = true,
    type = ConfigDef.Type.STRING,
    description = "A known kafka broker. Does not have to be the leader of the partition",
    label = "Broker Host",
    group = "KAFKA_PROPERTIES",
    defaultValue = "localhost")
  public String brokerHost;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    description = "Port number of the known broker host supplied above. Does not have to be the leader of the partition",
    label = "Broker Port",
    group = "KAFKA_PROPERTIES",
    defaultValue = "9092")
  public int brokerPort;

  @ConfigDef(required = true,
    type = ConfigDef.Type.BOOLEAN,
    description = "Reads messages from the beginning if set to true",
    label = "From Beginning",
    group = "KAFKA_PROPERTIES",
    defaultValue = "false")
  public boolean fromBeginning;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    description = "The maximum data per batch. The source uses this size when making a fetch request from kafka",
    label = "Max Fetch Size",
    group = "KAFKA_PROPERTIES",
    defaultValue = "64000")
  public int maxBatchSize;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    description = "The maximum wait time in seconds before the kafka fetch request returns if no message is available.",
    label = "Max Wait Time",
    group = "KAFKA_PROPERTIES",
    defaultValue = "1000")
  public int maxWaitTime;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    description = "The minimum data per batch. The source uses this size when making a fetch request from kafka",
    label = "Min Fetch Size",
    group = "KAFKA_PROPERTIES",
    defaultValue = "8000")
  public int minBatchSize;

  @ConfigDef(required = true,
    type = ConfigDef.Type.MODEL,
    description = "Type of data sent as kafka message payload",
    label = "Payload Type",
    group = "KAFKA_PROPERTIES",
    defaultValue = "TEXT")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = ConsumerPayloadTypeChooserValues.class)
  public ConsumerPayloadType consumerPayloadType;



  /****************** End config options *******************/

  private KafkaConsumer kafkaConsumer;

  @Override
  public void init() throws StageException {
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
      long latestOffsetInKafka = kafkaConsumer.getOffsetToRead(false);
      if(offsetToRead > latestOffsetInKafka) {
        LOG.error(KafkaStageLibError.KFK_0300.getMessage(), lastSourceOffset, latestOffsetInKafka, topic, partition);
        throw new StageException(KafkaStageLibError.KFK_0300, lastSourceOffset, latestOffsetInKafka, topic, partition);
      }
    }
    //This is where we want to start reading from kafka topic partition.
    //If no data is available in this batch then this is the offset we should look next time.
    //if offsetToReturn is initialized to null, the pipeline will terminate if no data is available
    String offsetToReturn = String.valueOf(offsetToRead);
    List<MessageAndOffset> partitionToPayloadList = new ArrayList<>();
    partitionToPayloadList.addAll(kafkaConsumer.read(offsetToRead));

    int recordCounter = 0;
    for(MessageAndOffset partitionToPayloadMap : partitionToPayloadList) {
      //create record by parsing the message payload based on the pay load type configuration
      //As of now handle just String
      if(recordCounter >= maxBatchSize) {
        //even though kafka has many messages, we need to cap the number of records to a value indicated by maxBatchSize.
        //return the offset of the previous record so that the next time we get start from this message which did not
        //make it to this batch.
        break;
      }
      List<Record> records = createRecords(partitionToPayloadMap, recordCounter);
      recordCounter += records.size();
      for(Record record : records) {
        batchMaker.addRecord(record);
      }
      offsetToReturn = String.valueOf(partitionToPayloadMap.getOffset());
    }
    return offsetToReturn;
  }

  @Override
  public void destroy() {
    kafkaConsumer.destroy();
    super.destroy();
  }

  protected abstract List<Record> createRecords(MessageAndOffset message, int currentRecordCount) throws StageException;

}
