/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.Errors;
import com.streamsets.pipeline.lib.KafkaBroker;
import com.streamsets.pipeline.lib.KafkaUtil;

public class StandaloneKafkaSource extends BaseKafkaSource {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneKafkaSource.class);
  private KafkaConsumer kafkaConsumer;

  public StandaloneKafkaSource(SourceArguments args) {
    super(args);
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
  List<ConfigIssue> issues =  new ArrayList<ConfigIssue>();

  List<KafkaBroker> kafkaBrokers = KafkaUtil.validateBrokerList(issues, zookeeperConnect, Groups.KAFKA.name(),
    "zookeeperConnect", getContext());

   //validate connecting to kafka
   if(kafkaBrokers != null && !kafkaBrokers.isEmpty() && topic !=null && !topic.isEmpty()) {
     kafkaConsumer = new KafkaConsumer(zookeeperConnect, topic, consumerGroup, maxBatchSize, maxWaitTime,
       kafkaConsumerConfigs, getContext());
     kafkaConsumer.validate(issues, getContext());
   }

   //consumerGroup
   if(consumerGroup == null || consumerGroup.isEmpty()) {
     issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "consumerGroup",
       Errors.KAFKA_33));
   }
   return validateCommonConfigs(issues);
  }

  @Override
  public void init() throws StageException {
    if(getContext().isPreview()) {
      //set fixed batch duration time of 1 second for preview.
      maxWaitTime = 1000;
    }
    kafkaConsumer.init();
    LOG.info("Successfully initialized Kafka Consumer");
  }


  private String getMessageID(MessageAndOffset message) {
    return topic + "::" + message.getPartition() + "::" + message.getOffset();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int recordCounter = 0;
    int batchSize = this.maxBatchSize > maxBatchSize ? maxBatchSize : this.maxBatchSize;
    long startTime = System.currentTimeMillis();
    while(recordCounter < batchSize && (startTime + maxWaitTime) > System.currentTimeMillis()) {
      MessageAndOffset message = kafkaConsumer.read();
      if (message != null) {
        String messageId = getMessageID(message);
        List<Record> records = processKafkaMessage(messageId, message.getPayload());
        for (Record record : records) {
          batchMaker.addRecord(record);
        }
        recordCounter += records.size();
      }
    }
    return lastSourceOffset;
  }

  @Override
  public void destroy() {
    kafkaConsumer.destroy();
  }

  @Override
  public void commit(String offset) throws StageException {
    kafkaConsumer.commit();
  }
}
