/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.Errors;
import com.streamsets.pipeline.lib.KafkaUtil;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.stage.origin.kafka.BaseKafkaSource;
import com.streamsets.pipeline.stage.origin.kafka.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Ingests kafka produce data from spark streaming
 */
public class SparkStreamingKafkaSource extends BaseKafkaSource implements OffsetCommitter, SparkStreamingSource {
  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingKafkaSource.class);

  private SparkStreamingQueue sparkStreamingQueue;
  private SparkStreamingQueueConsumer sparkStreamingQueueConsumer;
  private int originParallelism = 0;

  public SparkStreamingKafkaSource(String metadataBrokerList, String topic, DataFormat dataFormat, String charset,
    boolean produceSingleRecordPerMessage, int maxBatchSize, int maxWaitTime, Map<String, String> kafkaConsumerConfigs,
    int textMaxLineLen, JsonMode jsonContent, int jsonMaxObjectLen, CsvMode csvFileFormat, CsvHeader csvHeader,
    int csvMaxObjectLen, String xmlRecordElement, int xmlMaxObjectLen, LogMode logMode, int logMaxObjectLen,
    boolean retainOriginalLine, String customLogFormat, String regex, List<RegExConfig> fieldPathsToGroupName,
    String grokPatternDefinition, String grokPattern, boolean enableLog4jCustomLogFormat, String log4jCustomLogFormat,
    OnParseError onParseError, int maxStackTraceLines) {
    super(metadataBrokerList, null, null, topic, dataFormat, charset, produceSingleRecordPerMessage, maxBatchSize,
      maxWaitTime, kafkaConsumerConfigs, textMaxLineLen, jsonContent, jsonMaxObjectLen, csvFileFormat, csvHeader,
      csvMaxObjectLen, xmlRecordElement, xmlMaxObjectLen, logMode, logMaxObjectLen, retainOriginalLine,
      customLogFormat, regex, fieldPathsToGroupName, grokPatternDefinition, grokPattern, enableLog4jCustomLogFormat,
      log4jCustomLogFormat, onParseError, maxStackTraceLines);
    this.sparkStreamingQueue = new SparkStreamingQueue();
    this.sparkStreamingQueueConsumer = new SparkStreamingQueueConsumer(sparkStreamingQueue);
  }

  private String getRecordId(String topic) {
    return "spark-streaming" + "::" + topic;
  }

  @Override
  public List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = validateCommonConfigs(new ArrayList<ConfigIssue>());
    try {
      int partitionCount = KafkaUtil.getPartitionCount(metadataBrokerList, topic, 1, 0);
      if(partitionCount < 1) {
        issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic",
          Errors.KAFKA_42, topic));
      } else {
        //cache the partition count as parallelism for future use
        originParallelism = partitionCount;
      }
    } catch (StageException e) {
      issues.add(getContext().createConfigIssue(Groups.KAFKA.name(), "topic",
        Errors.KAFKA_41, topic, e.getMessage(), e));
    }
    return issues;
  }

  @Override
  public int getParallelism() throws StageException {
    if(originParallelism == 0) {
      //origin parallelism is not yet calculated
      originParallelism = KafkaUtil.getPartitionCount(metadataBrokerList, topic, 1, 0);
    }
    return originParallelism;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    // Ignore the batch size
    LOG.info("Source is " + this);
    OffsetAndResult<MessageAndPartition> offsetAndResult = sparkStreamingQueueConsumer.produce(maxBatchSize);
    for (MessageAndPartition messageAndPartition : offsetAndResult.getResult()) {
      String messageId = getRecordId(topic);
      List<Record> records = super.processKafkaMessage(messageId, messageAndPartition.getPayload());
      for (Record record : records) {
        batchMaker.addRecord(record);
      }
    }
    return offsetAndResult.getOffset();
  }

  @Override
  public long getRecordsProduced() {
    return sparkStreamingQueueConsumer.getRecordsProduced();
  }

  @Override
  public void init() {
    //
  }

  @Override
  public void destroy() {
    //
  }

  @Override
  public void commit(String offset) throws StageException {
    sparkStreamingQueueConsumer.commit(offset);
  }

  @Override
  public <T> void put(List<T> batch) throws InterruptedException {
    sparkStreamingQueue.putData(batch);
  }

}
