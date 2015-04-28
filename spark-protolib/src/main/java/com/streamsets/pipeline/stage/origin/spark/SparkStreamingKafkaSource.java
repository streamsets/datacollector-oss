/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import com.google.common.base.Throwables;
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
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.stage.origin.kafka.BaseKafkaSource;

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
  private int recordsProduced;

  private SparkStreamingQueue sparkStreamingQueue;

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
  }

  private String getRecordId(String topic) {
    return "spark-streaming" + "::" + topic;
  }

  @Override
  public List<ConfigIssue> validateConfigs() throws StageException {
    return validateCommonConfigs(new ArrayList<ConfigIssue>());
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    // Ignore the batch size
    Throwable error = null;
    LOG.info("Source is " + this);
    try {
      Object object;
      if ((object = sparkStreamingQueue.getElement(maxWaitTime)) != null) {
        List<MessageAndPartition> batch = null;
        if (object instanceof List) {
          batch = (List) object;
        } else {
          throw new IllegalStateException("Producer expects List, got " + object.getClass().getSimpleName());
        }
        for (MessageAndPartition messageAndPartition : batch) {
          String messageId = getRecordId(topic);
          List<Record> records = super.processKafkaMessage(messageId, messageAndPartition.getPayload());
          for (Record record : records) {
            batchMaker.addRecord(record);
          }
          recordsProduced += records.size();
        }
      } else {
        LOG.debug("Didn't get any data, must be empty RDD");
      }
    } catch (Throwable throwable) {
      error = throwable;
    } finally {
      if (error != null) {
        try {
          sparkStreamingQueue.putElement(error);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        if (error instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
    if (error != null) {
      Throwables.propagate(error);
    }
    return lastSourceOffset;
  }

  @Override
  public long getRecordsProduced() {
    return recordsProduced;
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
    sparkStreamingQueue.commit(offset);
  }

  @Override
  public <T> void put(List<T> batch) throws InterruptedException {
    sparkStreamingQueue.put(batch);
  }

}
