/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.lib.stage.source.spooldir.csv.CvsFileModeChooserValues;
import com.streamsets.pipeline.lib.stage.source.util.CsvUtil;
import com.streamsets.pipeline.lib.stage.source.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  /********  For CSV Content  ***********/

  @ConfigDef(required = false,
    type = ConfigDef.Type.MODEL,
    label = "CSV Format",
    description = "The specific CSV format of the files",
    defaultValue = "DEFAULT")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CvsFileModeChooserValues.class)
  public String csvFileFormat;

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
      try {
        kafkaProducer.enqueueMessage(serializeRecord(r));
      } catch (IOException e) {
        throw new StageException(null, e.getMessage(), e);
      }
      recordCounter++;
    }
    kafkaProducer.write();
  }

  @Override
  public void destroy() {
    LOG.info("Wrote {} number of records to Kafka Broker", recordCounter);
  }

  private byte[] serializeRecord(Record r) throws IOException {
    if(payloadType == PayloadType.STRING) {
      return r.get().getValue().toString().getBytes();
    } if (payloadType == PayloadType.JSON) {
      return JsonUtil.jsonRecordToString(r).getBytes();
    } if (payloadType == PayloadType.CSV) {
      return CsvUtil.csvRecordToString(r, CvsFileModeChooserValues.getCSVFormat(csvFileFormat)).getBytes();
    }
    return null;
  }

  @VisibleForTesting
  KafkaProducer getKafkaProducer() {
    return kafkaProducer;
  }
}
