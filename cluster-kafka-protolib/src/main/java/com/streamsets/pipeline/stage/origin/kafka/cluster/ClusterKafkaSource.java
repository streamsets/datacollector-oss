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
package com.streamsets.pipeline.stage.origin.kafka.cluster;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.cluster.Consumer;
import com.streamsets.pipeline.cluster.ControlChannel;
import com.streamsets.pipeline.cluster.DataChannel;
import com.streamsets.pipeline.cluster.Producer;
import com.streamsets.pipeline.impl.OffsetAndResult;
import com.streamsets.pipeline.stage.origin.kafka.BaseKafkaSource;
import com.streamsets.pipeline.stage.origin.kafka.KafkaConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Ingests kafka produce data from spark streaming
 */
public class ClusterKafkaSource extends BaseKafkaSource implements OffsetCommitter, ClusterSource, ErrorListener {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterKafkaSource.class);
  private static final String NO_OF_PARTITIONS = "partitionCount";
  private final ControlChannel controlChannel;
  private final DataChannel dataChannel;
  private final Producer producer;
  private final Consumer consumer;
  private long recordsProduced;

  public ClusterKafkaSource(KafkaConfigBean conf) {
    super(conf);
    controlChannel = new ControlChannel();
    dataChannel = new DataChannel();
    producer = new Producer(controlChannel, dataChannel);
    consumer = new Consumer(controlChannel, dataChannel);
    this.recordsProduced = 0;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    // Ignore the batch size
    OffsetAndResult<Map.Entry> offsetAndResult = consumer.take();
    long offset = (Long)offsetAndResult.getOffset();
    String messageId = String.format("kafka::%s::unknown", offset); // don't inc as we have not progressed
    for (Map.Entry  messageAndPartition : offsetAndResult.getResult()) {
      messageId = String.format("kafka::%s::%d", conf.topic, offset);
      List<Record> records = processKafkaMessageDefault(
          new String((byte[]) messageAndPartition.getKey()),
          offset,
          messageId,
          (byte[]) messageAndPartition.getValue()
      );
      offset++;
      for (Record record : records) {
        batchMaker.addRecord(record);
      }
    }
    return messageId;
  }

  @Override
  public long getRecordsProduced() {
    return recordsProduced;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> errors = super.init();
    if (errors.isEmpty()) {
      LOG.info("Successfully initialized Spark Kafka Consumer");
    }
    return errors;
  }

  @Override
  public void destroy() {
    shutdown();
    super.destroy();
  }

  @Override
  public void commit(String offset) throws StageException {
    consumer.commit(offset);
  }

  @Override
  public Object put(List<Map.Entry> batch) throws InterruptedException {
    Object expectedOffset = producer.put(new OffsetAndResult<>(recordsProduced, batch));
    recordsProduced += batch.size();
    return expectedOffset;
  }

  @Override
  public void completeBatch() throws InterruptedException {
    producer.waitForCommit();
  }

  @Override
  public void errorNotification(Throwable throwable) {
    consumer.error(throwable);
  }

  @Override
  public boolean inErrorState() {
    return producer.inErrorState() || consumer.inErrorState();
  }

  @Override
  public String getName() {
    return "kafka";
  }

  @Override
  public boolean isInBatchMode() {
    return false;
  }

  @Override
  public Map<String, String> getConfigsToShip() {
    try {
      Map<String, String> configBeanPrefixedMap = new HashMap<>();
      conf.kafkaConsumerConfigs.forEach((k, v) -> configBeanPrefixedMap.put(
          ClusterModeConstants.EXTRA_KAFKA_CONFIG_PREFIX + k,
          v
      ));
      return new ImmutableMap.Builder<String, String>().put(NO_OF_PARTITIONS, String.valueOf(getParallelism())).putAll(
          configBeanPrefixedMap).build();
    } catch (StageException e) {
      // Won't happen as getParallelism is already called which would have caught and bubbled as exception.
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
    producer.complete();
  }

  @Override
  public void postDestroy() {
    //don't do anything
  }

}
