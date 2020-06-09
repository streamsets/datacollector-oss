/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.MessageAndOffsetWithTimestamp;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StandaloneKafkaSource extends BaseKafkaSource {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneKafkaSource.class);

  private boolean checkBatchSize = true;

  public StandaloneKafkaSource(KafkaConfigBean conf) {
    super(conf);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (issues.isEmpty()) {
      if (getContext().isPreview()) {
        //set fixed batch duration time of 1 second for preview.
        conf.maxWaitTime = 1000;
      }
      try {
        kafkaConsumer.init();
        LOG.info("Successfully initialized Kafka Consumer");
      } catch (StageException ex) {
        issues.add(getContext().createConfigIssue(null, null, ex.getErrorCode(), ex.getParams()));
      }
    }
    if(issues.isEmpty()) {
      LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_READ);
      event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.KAFKA.name());
      event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, conf.topic);
      event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, conf.consumerGroup);
      getContext().publishLineageEvent(event);
    }

    return issues;
  }

  private String getMessageID(MessageAndOffset message) {
    return conf.topic + "::" + message.getPartition() + "::" + message.getOffset();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int recordCounter = 0;
    int batchSize = conf.maxBatchSize > maxBatchSize ? maxBatchSize : conf.maxBatchSize;
    if (!getContext().isPreview() && checkBatchSize && conf.maxBatchSize > maxBatchSize) {
      getContext().reportError(KafkaErrors.KAFKA_78, maxBatchSize);
      checkBatchSize = false;
    }

    long startTime = System.currentTimeMillis();
    while (recordCounter < batchSize && (startTime + conf.maxWaitTime) > System.currentTimeMillis()) {
      MessageAndOffset message = kafkaConsumer.read();
      if (message != null) {
        String messageId = getMessageID(message);
        List<Record> records;
        if (conf.timestampsEnabled && message instanceof MessageAndOffsetWithTimestamp){
          records = processKafkaMessageDefault(
              message.getMessageKey(),
              String.valueOf(message.getPartition()),
              message.getOffset(),
              messageId,
              (byte[]) message.getPayload(),
              ((MessageAndOffsetWithTimestamp) message).getTimestamp(),
              ((MessageAndOffsetWithTimestamp) message).getTimestampType()
          );
        }else{
          records = processKafkaMessageDefault(
              message.getMessageKey(),
              String.valueOf(message.getPartition()),
              message.getOffset(),
              messageId,
              (byte[]) message.getPayload()
          );
        }

        // If we are in preview mode, make sure we don't send a huge number of messages.
        if (getContext().isPreview() && recordCounter + records.size() > batchSize) {
          records = records.subList(0, batchSize - recordCounter);
        }
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
    if (kafkaConsumer != null) {
      kafkaConsumer.destroy();
    }
    super.destroy();
  }

  @Override
  public void commit(String offset) throws StageException {
    kafkaConsumer.commit();
  }
}
