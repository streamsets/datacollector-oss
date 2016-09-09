/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.google.common.base.Splitter;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil;
import com.streamsets.pipeline.stage.lib.kinesis.RecordsAndCheckpointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN;
import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.ONE_MB;

public class KinesisSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);
  private static final Splitter.MapSplitter offsetSplitter = Splitter.on("::").limit(2).withKeyValueSeparator("=");
  private static final String KINESIS_DATA_FORMAT_CONFIG_PREFIX = "kinesisConfig.dataFormatConfig.";
  private final KinesisConsumerConfigBean conf;

  private ExecutorService executorService;
  private boolean isStarted = false;
  private Worker worker;
  private BlockingQueue<RecordsAndCheckpointer> batchQueue;
  private IRecordProcessorCheckpointer checkpointer;
  private ErrorRecordHandler errorRecordHandler;
  private DataParserFactory parserFactory;

  public KinesisSource(KinesisConsumerConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    if (conf.region == AWSRegions.OTHER && (conf.endpoint == null || conf.endpoint.isEmpty())) {
      issues.add(getContext().createConfigIssue(
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN + ".endpoint",
          Errors.KINESIS_09
      ));
      return issues;
    }

    KinesisUtil.checkStreamExists(conf.region.getLabel(), conf.streamName, conf.awsConfig, issues, getContext());

    if (issues.isEmpty()) {
      batchQueue = new ArrayBlockingQueue<>(1);

      conf.dataFormatConfig.init(
          getContext(),
          conf.dataFormat,
          Groups.KINESIS.name(),
          KINESIS_DATA_FORMAT_CONFIG_PREFIX,
          ONE_MB,
          issues
      );

      parserFactory = conf.dataFormatConfig.getParserFactory();

      executorService = Executors.newFixedThreadPool(1);

      IRecordProcessorFactory recordProcessorFactory = new StreamSetsRecordProcessorFactory(batchQueue);

      // Create the KCL worker with the StreamSets record processor factory
      worker = createKinesisWorker(recordProcessorFactory);
    }
    return issues;
  }

  private Worker createKinesisWorker(IRecordProcessorFactory recordProcessorFactory) {
    KinesisClientLibConfiguration kclConfig =
        new KinesisClientLibConfiguration(
            conf.applicationName,
            conf.streamName,
            AWSUtil.getCredentialsProvider(conf.awsConfig),
            getWorkerId()
        );

    kclConfig
        .withMaxRecords(conf.maxBatchSize)
        .withIdleTimeBetweenReadsInMillis(conf.idleTimeBetweenReads)
        .withInitialPositionInStream(conf.initialPositionInStream)
        .withKinesisClientConfig(AWSUtil.getClientConfiguration(conf.proxyConfig));

    if (conf.region == AWSRegions.OTHER) {
      kclConfig.withKinesisEndpoint(conf.endpoint);
    } else {
      kclConfig.withRegionName(conf.region.getLabel());
    }

    return new Worker.Builder()
        .recordProcessorFactory(recordProcessorFactory)
        .config(kclConfig)
        .build();
  }

  private String getWorkerId() {
    String hostname = "unknownHostname";
    try {
      hostname = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException ignored) {
      // ignored
    }
    return hostname + ":" + UUID.randomUUID();
  }

  @Override
  public void destroy() {
    if (worker != null) {
      LOG.info("Shutting down worker for application {}", worker.getApplicationName());
      worker.shutdown();
    }
    if (executorService != null) {
      try {
        executorService.shutdown();
        if (!executorService.awaitTermination(conf.maxWaitTime, TimeUnit.MILLISECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while terminating executor service.", e);
      }
      if (!batchQueue.isEmpty()) {
        LOG.error("Queue still had {} batches at shutdown.", batchQueue.size());
      } else {
        LOG.info("Queue was empty at shutdown. No data lost.");
      }
    }
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    LOG.debug("Produce called.");
    if (!isStarted) {
      executorService.execute(worker);
      isStarted = true;
      LOG.info("Launched KCL Worker for application: {}", worker.getApplicationName());
    }

    int recordCounter = 0;
    long startTime = System.currentTimeMillis();
    long waitTime = conf.maxWaitTime;

    if (getContext().isPreview()) {
      waitTime = conf.previewWaitTime;
    }

    checkpointer = null;
    RecordsAndCheckpointer recordsAndCheckpointer = null;
    com.amazonaws.services.kinesis.model.Record record = null;
    while ((startTime + waitTime) > System.currentTimeMillis() && recordCounter < maxBatchSize) {
      try {
        long timeRemaining = (startTime + waitTime) - System.currentTimeMillis();
        recordsAndCheckpointer = batchQueue.poll(timeRemaining, TimeUnit.MILLISECONDS);
        if (null != recordsAndCheckpointer) {
          final List<com.amazonaws.services.kinesis.model.Record> batch = recordsAndCheckpointer.getRecords();

          if (batch.isEmpty()) {
            // Signaled that this is the end of a shard.
            lastSourceOffset = ExtendedSequenceNumber.SHARD_END.toString();
            checkpointer = recordsAndCheckpointer.getCheckpointer();
            break;
          }

          for (int index = 0; index < batch.size(); index++) {
            record = batch.get(index);
            batchMaker.addRecord(processKinesisRecord(record));
            ++recordCounter;
          }


        }
      } catch (IOException | DataParserException e) {
        LOG.warn(Errors.KINESIS_03.getMessage(), lastSourceOffset, e.toString(), e);
        errorRecordHandler.onError(Errors.KINESIS_03, lastSourceOffset, e);
      } catch (InterruptedException ignored) {
        // pipeline shutdown request.
      } finally {
        if (recordsAndCheckpointer != null) {
          if (!recordsAndCheckpointer.getRecords().isEmpty() && record != null) {
            checkpointer = recordsAndCheckpointer.getCheckpointer();
            lastSourceOffset = "sequenceNumber=" + record.getSequenceNumber() + "::" +
                "subSequenceNumber=" + ((UserRecord) record).getSubSequenceNumber();
          }
        }
      }
    }
    if (checkpointer == null) {
      LOG.debug("Checkpointer was null as there were no new records.");
    }

    return lastSourceOffset;
  }

  protected IRecordProcessorCheckpointer getCheckpointer() {
    return checkpointer;
  }

  private Record processKinesisRecord(com.amazonaws.services.kinesis.model.Record kRecord)
      throws DataParserException, IOException {
    final String recordId = createKinesisRecordId(kRecord);
    DataParser parser = parserFactory.getParser(recordId, kRecord.getData().array());
    return parser.parse();
  }

  private String createKinesisRecordId(com.amazonaws.services.kinesis.model.Record record) {
    return conf.streamName + "::" + record.getPartitionKey() + "::" + record.getSequenceNumber() + "::" +
        ((UserRecord) record).getSubSequenceNumber();
  }

  @Override
  public void commit(String offset) throws StageException {
    final boolean isPreview = getContext().isPreview();
    if (null != checkpointer && !isPreview && !offset.isEmpty()) {
      try {
        LOG.debug("Checkpointing batch at offset {}", offset);
        if (offset.equals(ExtendedSequenceNumber.SHARD_END.toString())) {
          KinesisUtil.checkpoint(checkpointer);
        } else {
          Map<String, String> offsets = offsetSplitter.split(offset);
          KinesisUtil.checkpoint(
              checkpointer, offsets.get("sequenceNumber"), Long.parseLong(offsets.get("subSequenceNumber"))
          );
        }
      } catch (NumberFormatException e) {
        // Couldn't parse the provided subsequence, invalid offset string.
        LOG.error("Couldn't parse the offset string: {}", offset);
        throw new StageException(Errors.KINESIS_04, offset);
      }
    } else if(isPreview) {
      LOG.debug("Not checkpointing because this origin is in preview mode.");
    }
  }
}
