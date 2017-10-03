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
package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN;
import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.ONE_MB;

public class KinesisSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);
  private static final String KINESIS_DATA_FORMAT_CONFIG_PREFIX = "kinesisConfig.dataFormatConfig.";
  private final KinesisConsumerConfigBean conf;
  private final BlockingQueue<Throwable> error = new SynchronousQueue<>();

  private DataParserFactory parserFactory;
  private ExecutorService executor;

  private ClientConfiguration clientConfiguration;
  private AWSCredentialsProvider credentials;
  private AmazonDynamoDB dynamoDBClient;
  private AmazonCloudWatch cloudWatchClient;
  private IMetricsFactory metricsFactory = null;
  private Worker worker;
  private AtomicBoolean resetOffsetAttempted;

  public KinesisSource(KinesisConsumerConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (conf.region == AWSRegions.OTHER && (conf.endpoint == null || conf.endpoint.isEmpty())) {
      issues.add(getContext().createConfigIssue(
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN + ".endpoint",
          Errors.KINESIS_09
      ));

      return issues;
    }

    try {
      KinesisUtil.checkStreamExists(
          AWSUtil.getClientConfiguration(conf.proxyConfig),
          conf,
          conf.streamName,
          issues,
          getContext()
      );
    } catch (StageException ex) {
      LOG.error(Utils.format(Errors.KINESIS_12.getMessage(), ex.toString()), ex);
      issues.add(getContext().createConfigIssue(
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN + ".awsConfig.awsAccessKeyId",
          Errors.KINESIS_12,
          ex.toString()
      ));
    }

    conf.dataFormatConfig.stringBuilderPoolSize = getNumberOfThreads();

    if (issues.isEmpty()) {
      conf.dataFormatConfig.init(
          getContext(),
          conf.dataFormat,
          Groups.KINESIS.name(),
          KINESIS_DATA_FORMAT_CONFIG_PREFIX,
          ONE_MB,
          issues
      );

      parserFactory = conf.dataFormatConfig.getParserFactory();
    }

    try {
      clientConfiguration = AWSUtil.getClientConfiguration(conf.proxyConfig);
      credentials = AWSUtil.getCredentialsProvider(conf.awsConfig);
    } catch (StageException ex) {
      LOG.error(Utils.format(Errors.KINESIS_12.getMessage(), ex.toString()), ex);
      issues.add(getContext().createConfigIssue(
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN + ".awsConfig.awsAccessKeyId",
          Errors.KINESIS_12,
          ex.toString()
      ));
    }

    // KCL currently requires a mutable client
    dynamoDBClient = new AmazonDynamoDBClient(credentials, clientConfiguration);
    cloudWatchClient = new AmazonCloudWatchClient(credentials, clientConfiguration);
    if (conf.region == AWSRegions.OTHER) {
      dynamoDBClient.setEndpoint(conf.endpoint);
      cloudWatchClient.setEndpoint(conf.endpoint);
    } else {
      Region region = Region.getRegion(Regions.valueOf(conf.region.name()));
      dynamoDBClient.setRegion(region);
      cloudWatchClient.setRegion(region);
    }

    resetOffsetAttempted = new AtomicBoolean(false);

    return issues;
  }

  @VisibleForTesting
  void setDynamoDBClient(AmazonDynamoDB client) {
    this.dynamoDBClient = client;
  }

  @VisibleForTesting
  void setMetricsFactory(IMetricsFactory metricsFactory) {
    this.metricsFactory = metricsFactory;
  }

  private Worker createKinesisWorker(IRecordProcessorFactory recordProcessorFactory, int maxBatchSize) {
    KinesisClientLibConfiguration kclConfig =
        new KinesisClientLibConfiguration(
            conf.applicationName,
            conf.streamName,
            credentials,
            getWorkerId()
        );

    kclConfig
        .withMaxRecords(maxBatchSize)
        .withCallProcessRecordsEvenForEmptyRecordList(false)
        .withIdleTimeBetweenReadsInMillis(conf.idleTimeBetweenReads)
        .withInitialPositionInStream(conf.initialPositionInStream)
        .withKinesisClientConfig(clientConfiguration);

    if (conf.initialPositionInStream == InitialPositionInStream.AT_TIMESTAMP) {
      kclConfig.withTimestampAtInitialPositionInStream(new Date(conf.initialTimestamp));
    }

    if (conf.region == AWSRegions.OTHER) {
      kclConfig.withKinesisEndpoint(conf.endpoint);
    } else {
      kclConfig.withRegionName(conf.region.getLabel());
    }

    return new Worker.Builder()
        .recordProcessorFactory(recordProcessorFactory)
        .metricsFactory(metricsFactory)
        .dynamoDBClient(dynamoDBClient)
        .cloudWatchClient(cloudWatchClient)
        .execService(executor)
        .config(kclConfig)
        .build();
  }

  private String getWorkerId() {
    String hostname = "unknownHostname";
    try {
      hostname = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException ignored) { // NOSONAR
      // ignored
    }
    return hostname + ":" + UUID.randomUUID();
  }

  private void previewProcess(
    int maxBatchSize,
    BatchMaker batchMaker
  ) throws IOException, StageException {
    ClientConfiguration awsClientConfig = AWSUtil.getClientConfiguration(conf.proxyConfig);

    String shardId = KinesisUtil.getLastShardId(awsClientConfig, conf, conf.streamName);

    GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
    getShardIteratorRequest.setStreamName(conf.streamName);
    getShardIteratorRequest.setShardId(shardId);
    getShardIteratorRequest.setShardIteratorType(conf.initialPositionInStream.name());

    List<com.amazonaws.services.kinesis.model.Record> results = KinesisUtil.getPreviewRecords(
        awsClientConfig,
        conf,
        Math.min(conf.maxBatchSize, maxBatchSize),
        getShardIteratorRequest
    );

    int batchSize = results.size() > maxBatchSize ? maxBatchSize : results.size();

    for (int index = 0; index < batchSize; index++) {
      com.amazonaws.services.kinesis.model.Record record = results.get(index);
      UserRecord userRecord = new UserRecord(record);
      KinesisUtil.processKinesisRecord(
          getShardIteratorRequest.getShardId(),
          userRecord,
          parserFactory
      ).forEach(batchMaker::addRecord);
    }
  }

  @Override
  public void destroy() {
    Optional.ofNullable(worker).ifPresent(Worker::shutdown);
    Optional.ofNullable(executor).ifPresent(ExecutorService::shutdownNow);
    super.destroy();
  }

  @Override
  public int getNumberOfThreads() {
    // Since this executor service is also used for the Worker
    return conf.maxRecordProcessors + 1;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    if (getContext().isPreview()) {
      try {
        BatchContext previewBatchContext = getContext().startBatch();
        BatchMaker previewBatchMaker = previewBatchContext.getBatchMaker();
        previewProcess(maxBatchSize, previewBatchMaker);
        getContext().processBatch(previewBatchContext);
      } catch (IOException | DataParserException e) {
        throw new StageException(Errors.KINESIS_10, e.toString(), e);
      }
      return;
    }

    resetOffsets(lastOffsets);
    executor = Executors.newFixedThreadPool(getNumberOfThreads());
    IRecordProcessorFactory recordProcessorFactory = new StreamSetsRecordProcessorFactory(
        getContext(),
        parserFactory,
        Math.min(conf.maxBatchSize, maxBatchSize),
        error
    );

    // Create the KCL worker with the StreamSets record processor factory
    worker = createKinesisWorker(recordProcessorFactory, Math.min(conf.maxBatchSize, maxBatchSize));
    executor.submit(worker);
    LOG.info("Launched KCL Worker for application: {}", worker.getApplicationName());

    try {
      CompletableFuture.supplyAsync(() -> {
        while (!getContext().isStopped()) {
          // To handle OnError STOP_PIPELINE we keep checking for an exception thrown
          // by any record processor in order to perform a graceful shutdown.
          try {
            Throwable t = error.poll(100, TimeUnit.MILLISECONDS);
            if (t != null) {
              return Optional.of(t);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return Optional.<Throwable>empty();
      }).get().ifPresent(t -> {
        throw Throwables.propagate(t);
      });
    } catch (InterruptedException | ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  private void resetOffsets(Map<String, String> lastOffsets) throws StageException {
    if (lastOffsets.isEmpty() && !resetOffsetAttempted.getAndSet(true)) {
      DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
      Table offsetTable = dynamoDB.getTable(conf.applicationName);

      if (!tableExists(conf.applicationName)) {
        return;
      }

      try {
        offsetTable.delete();
        offsetTable.waitForDelete();
        LOG.info("Deleted DynamoDB table for application '{}' since reset offset was invoked.", conf.applicationName);
      } catch (AmazonDynamoDBException e) {
        LOG.error(Errors.KINESIS_11.getMessage(), conf.applicationName, e);
        throw new StageException(Errors.KINESIS_11, conf.applicationName, e);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for table '{}' deletion", conf.applicationName, e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private boolean tableExists(String tableName) {
    try {
      dynamoDBClient.describeTable(tableName);
      return true;
    } catch (ResourceNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Table'{}' did not exist.", tableName, e);
      }
      return false;
    }
  }
}
