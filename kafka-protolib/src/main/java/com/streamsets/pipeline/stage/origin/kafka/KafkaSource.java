/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.CsvRecordCreator;
import com.streamsets.pipeline.lib.JsonRecordCreator;
import com.streamsets.pipeline.lib.LogRecordCreator;
import com.streamsets.pipeline.lib.MessageAndOffset;
import com.streamsets.pipeline.lib.RecordCreator;
import com.streamsets.pipeline.lib.SDCRecordCreator;
import com.streamsets.pipeline.lib.XmlRecordCreator;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class KafkaSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  private final String zookeeperConnect;
  private final String consumerGroup;
  private final String topic;
  private final DataFormat consumerPayloadType;
  private final int maxBatchSize;
  private final Map<String, String> kafkaConsumerConfigs;
  private final StreamingJsonParser.Mode jsonContent;
  private final boolean produceSingleRecord;
  private final int maxJsonObjectLen;
  private final CsvMode csvFileFormat;
  private int maxWaitTime;
  private RecordCreator recordCreator;
  private KafkaConsumer kafkaConsumer;

  public KafkaSource(String zookeeperConnect, String consumerGroup, String topic,
                     DataFormat consumerPayloadType, int maxBatchSize, int maxWaitTime,
                     Map<String, String> kafkaConsumerConfigs, StreamingJsonParser.Mode jsonContent,
                     boolean produceSingleRecord, int maxJsonObjectLen, CsvMode csvFileFormat) {
    this.zookeeperConnect = zookeeperConnect;
    this.consumerGroup = consumerGroup;
    this.topic = topic;
    this.consumerPayloadType = consumerPayloadType;
    this.maxBatchSize = maxBatchSize;
    this.maxWaitTime = maxWaitTime;
    this.kafkaConsumerConfigs = kafkaConsumerConfigs;
    this.jsonContent = jsonContent;
    this.produceSingleRecord = produceSingleRecord;
    this.maxJsonObjectLen = maxJsonObjectLen;
    this.csvFileFormat = csvFileFormat;
  }

  @Override
  public void init() throws StageException {
    if(getContext().isPreview()) {
      //set fixed batch duration time of 1 second for preview.
      maxWaitTime = 1000;
    }
    kafkaConsumer = new KafkaConsumer(zookeeperConnect, topic, consumerGroup, maxBatchSize, maxWaitTime,
      kafkaConsumerConfigs, getContext());
    kafkaConsumer.init();
    LOG.info("Successfully initialized Kafka Consumer");

    switch ((consumerPayloadType)) {
      case JSON:
        recordCreator = new JsonRecordCreator(getContext(), jsonContent, maxJsonObjectLen, produceSingleRecord, topic);
        break;
      case TEXT:
        recordCreator = new LogRecordCreator(getContext(), topic);
        break;
      case DELIMITED:
        recordCreator = new CsvRecordCreator(getContext(), csvFileFormat, topic);
        break;
      case XML:
        recordCreator = new XmlRecordCreator(getContext(), topic);
        break;
      case SDC_JSON:
        recordCreator = new SDCRecordCreator(getContext());
        break;
      default :
    }
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    LOG.debug("Reading messages from kafka.");
    int recordCounter = 0;
    int batchSize = this.maxBatchSize > maxBatchSize ? maxBatchSize : this.maxBatchSize;
    long startTime = System.currentTimeMillis();
    while(recordCounter < batchSize && (startTime + maxWaitTime) > System.currentTimeMillis()) {
      MessageAndOffset message = kafkaConsumer.read();
      if(message != null) {
        List<Record> records = recordCreator.createRecords(message, recordCounter);
        recordCounter += records.size();
        for(Record record : records) {
          batchMaker.addRecord(record);
        }
      }
    }
    LOG.info("Produced {} records in this batch.", recordCounter);
    return lastSourceOffset;
  }

  @Override
  public void destroy() {
    LOG.info("Destroying Kafka Consumer");
    kafkaConsumer.destroy();
    super.destroy();
  }

  @Override
  public void commit(String offset) throws StageException {
    LOG.info("Committing offset for topic.");
    kafkaConsumer.commit();
  }

}
