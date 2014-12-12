/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

@GenerateResourceBundle
@StageDef(version="0.0.1",
  label="Kafka Target")
public class KafkaTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTarget.class);

  @ConfigDef(required = true,
    type = ConfigDef.Type.STRING,
    label = "Topic",
    defaultValue = "mytopic")
  public String topic;

  @ConfigDef(required = false,
    type = ConfigDef.Type.INTEGER,
    label = "Partition",
    defaultValue = "-1")
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
    type = ConfigDef.Type.MODEL,
    label = "Payload Type")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = PayloadTypeChooserValues.class)
  public PayloadType payloadType;

  @ConfigDef(required = true,
    type = ConfigDef.Type.MODEL,
    label = "Partition Strategy",
    defaultValue = "FIXED")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = PartitionStrategyChooserValues.class)
  public PartitionStrategy partitionStrategy;

  private KafkaProducer kafkaProducer;
  private long recordCounter = 0;

  @Override
  public void init() {
    if(partition != -1) {
      //If the value of partition is not default, then all messages are written to that topic
      //Otherwise, partition strategy determines how the messages are routed to different partitions.
      partitionStrategy = PartitionStrategy.FIXED;
    }
    kafkaProducer = new KafkaProducer(topic, String.valueOf(partition), new KafkaBroker(brokerHost, brokerPort),
      payloadType, partitionStrategy);
    kafkaProducer.init();
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    while(records.hasNext()) {
      Record r = records.next();
      kafkaProducer.enqueueRecord(r);
      recordCounter++;
    }
    kafkaProducer.write();
  }

  @Override
  public void destroy() {
    LOG.info("Wrote {} number of records to Kafka Broker", recordCounter);
  }

}
