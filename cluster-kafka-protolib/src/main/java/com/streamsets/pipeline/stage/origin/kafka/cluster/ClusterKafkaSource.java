/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka.cluster;

import com.streamsets.pipeline.impl.OffsetAndResult;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.cluster.Consumer;
import com.streamsets.pipeline.cluster.ControlChannel;
import com.streamsets.pipeline.cluster.DataChannel;
import com.streamsets.pipeline.cluster.Producer;
import com.streamsets.pipeline.stage.origin.kafka.BaseKafkaSource;
import com.streamsets.pipeline.stage.origin.kafka.SourceArguments;

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
  private final ControlChannel controlChannel;
  private final DataChannel dataChannel;
  private final Producer producer;
  private final Consumer consumer;
  private long recordsProduced;

  public ClusterKafkaSource(SourceArguments args) {
    super(args);
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
      messageId = String.format("kafka::%s::%d", topic, offset++);
      List<Record> records = processKafkaMessage(messageId, (byte[]) messageAndPartition.getValue());
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
  public void put(List<Map.Entry> batch) throws InterruptedException {
    producer.put(new OffsetAndResult<>(recordsProduced, batch));
    recordsProduced += batch.size();
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
    return new HashMap<String, String>();
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
