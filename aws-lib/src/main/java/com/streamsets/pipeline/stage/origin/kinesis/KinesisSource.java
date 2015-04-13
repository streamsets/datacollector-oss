/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

public class KinesisSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);

  private final Regions region;
  private final String streamName;
  private final DataFormat dataFormat;
  private final int maxBatchSize;
  private final long idleTimeBetweenReads;
  private final long maxWaitTime;
  private final long previewWaitTime;

  private ExecutorService executorService;
  private Worker worker;
  private LinkedTransferQueue<Pair<List<com.amazonaws.services.kinesis.model.Record>, IRecordProcessorCheckpointer>> batchQueue;
  private IRecordProcessorCheckpointer checkpointer;

  private DataParserFactory parserFactory;

  public KinesisSource(
      final Regions region,
      final String streamName,
      final DataFormat dataFormat,
      final int maxBatchSize,
      final long idleTimeBetweenReads,
      final long maxWaitTime,
      final long previewWaitTime,
      final String awsAccessKeyId,
      final String awsSecretAccessKey
  ) {
    this.region = region;
    this.streamName = streamName;
    this.dataFormat = dataFormat;
    this.maxBatchSize = maxBatchSize;
    this.idleTimeBetweenReads = idleTimeBetweenReads;
    this.maxWaitTime = maxWaitTime;
    this.previewWaitTime = previewWaitTime;

    System.setProperty("aws.accessKeyId", awsAccessKeyId);
    System.setProperty("aws.secretKey", awsSecretAccessKey);
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();

    checkStreamExists(issues);

    return issues;
  }

  private void checkStreamExists(List<ConfigIssue> issues) {
    ClientConfiguration kinesisConfiguration = new ClientConfiguration();
    AmazonKinesisClient kinesisClient = new AmazonKinesisClient(kinesisConfiguration);
    kinesisClient.setRegion(region);

    try {
      DescribeStreamResult result = kinesisClient.describeStream(streamName);
      LOG.info("Connected successfully to stream: {} with description: {}",
          streamName,
          result.getStreamDescription().toString()
      );
    } catch (Exception e) {
      issues.add(getContext().createConfigIssue(Groups.KINESIS.name(), "streamName", Errors.KINESIS_01, e.getMessage()));
    } finally {
      kinesisClient.shutdown();
    }
  }

  @Override
  protected void init() throws StageException {
    super.init();

    batchQueue = new LinkedTransferQueue<>();

    parserFactory = new DataParserFactoryBuilder(getContext(), dataFormat.getParserFormat())
        .setMaxDataLen(50 * 1024) // Max Message for Kinesis is 50KiB
        .build();

    executorService = Executors.newFixedThreadPool(1);

    IRecordProcessorFactory recordProcessorFactory = new StreamSetsRecordProcessorFactory(batchQueue);

    // Create the KCL worker with the StreamSets record processor factory
    KinesisClientLibConfiguration kclConfig =
        new KinesisClientLibConfiguration(
            "sdc",
            streamName,
            new DefaultAWSCredentialsProviderChain(),
            UUID.randomUUID().toString()
        );
    kclConfig
        .withRegionName(region.getName())
        .withMaxRecords(maxBatchSize)
        .withIdleTimeBetweenReadsInMillis(idleTimeBetweenReads)
        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON); // Configurable?

    worker = new Worker(recordProcessorFactory, kclConfig);

    // Launch our worker thread
    executorService.execute(worker);
    LOG.info("Launched KCL Worker");
  }

  @Override
  public void destroy() {
    super.destroy();
    worker.shutdown();
    try {
      executorService.awaitTermination(maxWaitTime, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {}
    if (!batchQueue.isEmpty()) {
      LOG.error("Queue still had {} batches at shutdown.", batchQueue.size());
    } else {
      LOG.info("Queue was empty at shutdown. No data lost.");
    }
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {

    int recordCounter = 0;
    long startTime = System.currentTimeMillis();
    long waitTime = maxWaitTime;

    if (getContext().isPreview()) {
      waitTime = previewWaitTime;
    }

    while((startTime + waitTime) > System.currentTimeMillis() && recordCounter < maxBatchSize) {
      try {
        long timeRemaining = (startTime + waitTime) - System.currentTimeMillis();
        final Pair<List<com.amazonaws.services.kinesis.model.Record>, IRecordProcessorCheckpointer> pair =
            batchQueue.poll(timeRemaining, TimeUnit.MILLISECONDS);
        if (null != pair) {
          final List<com.amazonaws.services.kinesis.model.Record> batch = pair.getLeft();
          checkpointer = pair.getRight();
          for (com.amazonaws.services.kinesis.model.Record record : batch) {
            batchMaker.addRecord(processKinesisRecord(record));
            lastSourceOffset = record.getSequenceNumber();
            ++recordCounter;
          }
        }
      } catch (IOException | DataParserException e) {
        handleErrorRecord(e, Errors.KINESIS_03, lastSourceOffset);
      } catch (InterruptedException ignored) {
        // pipeline shutdown request.
      }
    }
    return lastSourceOffset;
  }

  private Record processKinesisRecord(com.amazonaws.services.kinesis.model.Record kRecord)
      throws DataParserException, IOException {
    final String recordId = createKinesisRecordId(kRecord);
    DataParser parser = parserFactory.getParser(recordId, kRecord.getData().array());
    return parser.parse();
  }

  private String createKinesisRecordId(com.amazonaws.services.kinesis.model.Record record) {
    return streamName + "::" + record.getPartitionKey() + "::" + record.getSequenceNumber();
  }

  @Override
  public void commit(String offset) throws StageException {
    final boolean isPreview = getContext().isPreview();
    if (null != checkpointer && !isPreview && !offset.isEmpty()) {
      try {
        LOG.info("Checkpointing batch at offset {}", offset);
        checkpointer.checkpoint(offset);
      } catch (ShutdownException se) {
        // Ignore checkpoint if the processor instance has been shutdown (fail over).
        LOG.info("Caught shutdown exception, skipping checkpoint.", se);
      } catch (ThrottlingException e) {
        // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
        LOG.error("Caught throttling exception, skipping checkpoint.", e);
      } catch (InvalidStateException e) {
        // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
        LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
      }
    } else if(isPreview) {
      LOG.info("Not checkpointing because this origin is in preview mode.");
    }
  }

  private void handleErrorRecord(
      Throwable e,
      ErrorCode errorCode,
      Object... context
  ) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().reportError(errorCode, context);
        break;
      case STOP_PIPELINE:
        throw new StageException(errorCode, context);
      default:
        throw new IllegalStateException(
            Utils.format("It should never happen. OnError '{}'", getContext().getOnErrorRecord(), e)
        );
    }
  }
}
