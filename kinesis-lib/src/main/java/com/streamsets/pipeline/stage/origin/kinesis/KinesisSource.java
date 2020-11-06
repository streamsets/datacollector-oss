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

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.Tag;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseSerializer;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.lib.aws.AWSKinesisUtil;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.stage.lib.kinesis.AdditionalClientConfiguration;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
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
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN;
import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN_CONNECTION;
import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.ONE_MB;
import static org.awaitility.Awaitility.await;

public class KinesisSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);
  private static final long DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY = 10L;
  private static final long DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY = 10L;
  private static final String KINESIS_DATA_FORMAT_CONFIG_PREFIX = "kinesisConfig.dataFormatConfig.";
  private final KinesisConsumerConfigBean conf;
  private final BlockingQueue<Throwable> error = new SynchronousQueue<>();

  private DataParserFactory parserFactory;
  private ExecutorService executor;

  private ClientConfiguration clientConfiguration;
  private KinesisClientLibConfiguration kclConfig;
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

    if (conf.connection.region == AwsRegion.OTHER && (conf.connection.endpoint == null || conf.
        connection.endpoint.isEmpty())) {
      issues.add(getContext().createConfigIssue(
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN_CONNECTION + ".endpoint",
          Errors.KINESIS_09
      ));

      return issues;
    }

    Map<String, String> kinesisConsumerValidConfigurations = conf.kinesisConsumerConfigs;
    for (Map.Entry<String, String> property : conf.kinesisConsumerConfigs.entrySet()) {
      if (!AdditionalClientConfiguration.propertyExists(property.getKey())) {
        issues.add(getContext().createConfigIssue(Groups.KINESIS.name(),
            KINESIS_CONFIG_BEAN + ".kinesisConsumerConfigs",
            Errors.KINESIS_24,
            property.getKey()
        ));
        kinesisConsumerValidConfigurations.remove(property.getKey());
      }
    }

    boolean clientConfigurationCredentialsCorrect = true;
    clientConfiguration = AWSKinesisUtil.getClientConfiguration(conf.connection.proxyConfig);
    try {
      KinesisUtil.checkStreamExists(
          clientConfiguration,
          conf.connection,
          conf.streamName,
          issues,
          getContext()
      );
    } catch (StageException ex) {
      LOG.error(Utils.format(Errors.KINESIS_12.getMessage(), ex.toString()), ex);
      issues.add(getContext().createConfigIssue(
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN_CONNECTION + ".awsConfig.awsAccessKeyId",
          Errors.KINESIS_12,
          ex.toString()
      ));
      clientConfigurationCredentialsCorrect = false;
    }

    if (kinesisConsumerValidConfigurations != null) {
      AWSKinesisUtil.addAdditionalClientConfiguration(clientConfiguration,
          kinesisConsumerValidConfigurations,
          issues,
          getContext()
      );

      if (clientConfigurationCredentialsCorrect) {
        try {
          KinesisUtil.checkStreamExists(
              clientConfiguration,
              conf.connection,
              conf.streamName,
              issues,
              getContext()
          );
        } catch (StageException ex) {
          LOG.error(Utils.format(Errors.KINESIS_23.getMessage(), ex.toString()), ex);
          issues.add(getContext().createConfigIssue(Groups.KINESIS.name(),
              KINESIS_CONFIG_BEAN + ".kinesisConsumerConfigs",
              Errors.KINESIS_23,
              ex.toString()
          ));
        }
      }
    }

    conf.dataFormatConfig.stringBuilderPoolSize = getNumberOfThreads();

    if (issues.isEmpty()) {
      conf.dataFormatConfig.init(getContext(),
          conf.dataFormat,
          Groups.KINESIS.name(),
          KINESIS_DATA_FORMAT_CONFIG_PREFIX,
          ONE_MB,
          issues
      );

      parserFactory = conf.dataFormatConfig.getParserFactory();
    }

    issues.addAll(buildDynamoAndCloudWatchClients());

    resetOffsetAttempted = new AtomicBoolean(false);

    kclConfig = new KinesisClientLibConfiguration(conf.applicationName, conf.streamName, credentials, getWorkerId());
    if (kinesisConsumerValidConfigurations != null) {
      KinesisUtil.addAdditionalKinesisClientConfiguration(kclConfig,
          kinesisConsumerValidConfigurations,
          issues,
          getContext()
      );
    }

    return issues;
  }

  private List<ConfigIssue> buildDynamoAndCloudWatchClients() {
    List<ConfigIssue> issues = new ArrayList<>();
    try {
      // KCL currently requires a mutable client
      AmazonDynamoDBClientBuilder dynamoBuilder = AmazonDynamoDBClientBuilder.standard().withClientConfiguration(
          clientConfiguration);

      AmazonCloudWatchClientBuilder cloudWatchBuilder = AmazonCloudWatchClientBuilder.standard()
                                                                                     .withClientConfiguration(
                                                                                         clientConfiguration);

      Regions region = Regions.DEFAULT_REGION;

      if (conf.connection.region == AwsRegion.OTHER) {
        Matcher matcher = KinesisUtil.REGION_PATTERN.matcher(conf.connection.endpoint);
        if (matcher.find()) {
          region = Regions.fromName(matcher.group(1).toLowerCase());
        } else {
          issues.add(getContext().createConfigIssue(Groups.KINESIS.name(),
              KINESIS_CONFIG_BEAN_CONNECTION + ".endpoint",
              Errors.KINESIS_19
          ));
        }
      } else {
        region = Regions.fromName(conf.connection.region.getId().toLowerCase());
      }

      credentials = AWSKinesisUtil.getCredentialsProvider(conf.connection.awsConfig, getContext(), region);
      dynamoBuilder.withRegion(region).withCredentials(credentials);
      cloudWatchBuilder.withRegion(region).withCredentials(credentials);
      dynamoDBClient = dynamoBuilder.build();
      cloudWatchClient = cloudWatchBuilder.build();
    } catch (StageException ex) {
      LOG.error(Utils.format(Errors.KINESIS_12.getMessage(), ex.toString()), ex);
      issues.add(getContext().createConfigIssue(Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN_CONNECTION + ".awsConfig.awsAccessKeyId",
          Errors.KINESIS_12,
          ex.toString()
      ));
    }
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

  @VisibleForTesting
  Map<String, String> getAdditionalProperties() {
    Map<String, String> additionalProperties = new HashMap<>();
    additionalProperties.put("maxConsecutiveRetriesBeforeThrottling",
        Integer.toString(this.clientConfiguration.getMaxConnections())
    );
    additionalProperties.put("maxLeaseRenewalThreads", Integer.toString(this.kclConfig.getMaxLeaseRenewalThreads()));
    return additionalProperties;
  }

  private Worker createKinesisWorker(IRecordProcessorFactory recordProcessorFactory, int maxBatchSize) {
    kclConfig.withMaxRecords(maxBatchSize)
             .withCallProcessRecordsEvenForEmptyRecordList(false)
             .withIdleTimeBetweenReadsInMillis(conf.idleTimeBetweenReads)
             .withKinesisClientConfig(clientConfiguration);

    if (conf.initialPositionInStream == InitialPositionInStream.AT_TIMESTAMP) {
      kclConfig.withTimestampAtInitialPositionInStream(new Date(conf.initialTimestamp));
    } else if (conf.initialPositionInStream == InitialPositionInStream.LATEST ||
        conf.initialPositionInStream == InitialPositionInStream.TRIM_HORIZON) {
      kclConfig.withInitialPositionInStream(conf.initialPositionInStream);
    }

    if (conf.connection.region == AwsRegion.OTHER) {
      Matcher matcher = KinesisUtil.REGION_PATTERN.matcher(conf.connection.endpoint);
      if (matcher.find()) {
        kclConfig.withRegionName(matcher.group(1));
        kclConfig.withKinesisEndpoint(conf.connection.endpoint.substring(matcher.start(), matcher.end()));
      }
    } else {
      kclConfig.withRegionName(conf.connection.region.getId());
    }

    return new Worker.Builder().recordProcessorFactory(recordProcessorFactory)
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

  private void previewProcess(int maxBatchSize, BatchMaker batchMaker) throws IOException {
    String shardId = KinesisUtil.getLastShardId(clientConfiguration, conf, conf.streamName, getContext());

    GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
    getShardIteratorRequest.setStreamName(conf.streamName);
    getShardIteratorRequest.setShardId(shardId);
    getShardIteratorRequest.setShardIteratorType(conf.initialPositionInStream.name());

    if (conf.initialPositionInStream == InitialPositionInStream.AT_TIMESTAMP) {
      getShardIteratorRequest.setTimestamp(new Date(conf.initialTimestamp));
    }

    if (!getContext().isPreview() && conf.maxBatchSize > maxBatchSize) {
      getContext().reportError(Errors.KINESIS_18, maxBatchSize);
    }

    List<com.amazonaws.services.kinesis.model.Record> results = KinesisUtil.getPreviewRecords(
        clientConfiguration,
        conf,
        Math.min(conf.maxBatchSize, maxBatchSize),
        getShardIteratorRequest,
        getContext()
    );

    int batchSize = Math.min(results.size(), maxBatchSize);

    for (int index = 0; index < batchSize; index++) {
      com.amazonaws.services.kinesis.model.Record record = results.get(index);
      UserRecord userRecord = new UserRecord(record);
      KinesisUtil.processKinesisRecord(getShardIteratorRequest.getShardId(), userRecord, parserFactory).forEach(
          batchMaker::addRecord);
    }
  }

  private boolean shutDownResourcesAndReturnInterrupted() {
    AtomicBoolean interrupted = new AtomicBoolean(false);
    Optional.ofNullable(worker).ifPresent(w -> {
      try {
        boolean isStopped = w.startGracefulShutdown().get();
        if (!isStopped) {
          LOG.warn("Worker not stopped properly");
        }
      } catch (ExecutionException e) {
        LOG.error("Exception while shutting down the worker", e);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while shutting down workers");
        interrupted.set(true);
      }
    });

    Optional.ofNullable(executor).ifPresent(exe -> {
      if (!exe.isTerminated()) {
        LOG.info("Shutting down executor service");
        exe.shutdown();
        try {
          exe.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while shutting down executor");
          interrupted.set(true);
        }
      }
    });
    return interrupted.get();
  }

  @Override
  public void destroy() {
    if (shutDownResourcesAndReturnInterrupted()) {
      LOG.error("Interrupted while shutting down");
    }
    super.destroy();
  }

  @Override
  public int getNumberOfThreads() {
    // Since this executor service is also used for the Worker
    return conf.maxRecordProcessors + 1;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) {
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
    createLeaseTableIfNotExists();

    executor = Executors.newFixedThreadPool(getNumberOfThreads());
    IRecordProcessorFactory recordProcessorFactory = new StreamSetsRecordProcessorFactory(getContext(),
        parserFactory,
        Math.min(conf.maxBatchSize, maxBatchSize),
        error,
        conf.throttleWaitTime,
        conf.throttleMaxRetries
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

  private void createLeaseTableIfNotExists() {
    // Lease table doesn't need creation
    if (leaseTableExists()) {
      return;
    }

    DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
    KinesisClientLeaseSerializer leaseSerializer = new KinesisClientLeaseSerializer();
    CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(conf.applicationName)
                                                                    .withKeySchema(leaseSerializer.getKeySchema())
                                                                    .withAttributeDefinitions(leaseSerializer.getAttributeDefinitions())
                                                                    .withProvisionedThroughput(new ProvisionedThroughput()
                                                                        .withReadCapacityUnits(
                                                                            DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY)
                                                                        .withWriteCapacityUnits(
                                                                            DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY));

    try {
      Table leaseTable = dynamoDB.createTable(createTableRequest);
      LOG.debug("Waiting up to 2 minutes for table creation and readiness");
      await().atMost(2, TimeUnit.MINUTES).until(this::leaseTableExists);
      Collection<Tag> tags = conf.leaseTable.tags.entrySet()
                                                 .stream()
                                                 .map(e -> new Tag().withKey(e.getKey())
                                                                    .withValue(e.getValue()))
                                                 .collect(Collectors.toSet());

      if (!tags.isEmpty()) {
        TagResourceRequest tagRequest = new TagResourceRequest().withTags(tags)
                                                                .withResourceArn(leaseTable.getDescription()
                                                                                           .getTableArn());

        if (LOG.isInfoEnabled()) {
          LOG.info("Tagging lease table {} with tags: '{}'",
              conf.applicationName,
              Joiner.on(",").withKeyValueSeparator("=").join(conf.leaseTable.tags)
          );
        }
        dynamoDBClient.tagResource(tagRequest);
      }
    } catch (ResourceInUseException e) {
      // We're not expecting any lease table to exist since we've already checked for its existence.
      // In this case some other process may have created the table and we weren't expecting it.
      LOG.error(Errors.KINESIS_13.getMessage(), conf.applicationName);
      throw new StageException(Errors.KINESIS_13, conf.applicationName);
    } catch (LimitExceededException e) {
      LOG.error(Errors.KINESIS_14.getMessage(), conf.applicationName, e);
      throw new StageException(Errors.KINESIS_14, conf.applicationName, e);
    } catch (AmazonClientException e) {
      LOG.error(Errors.KINESIS_15.getMessage(), e.toString(), e);
      throw new StageException(Errors.KINESIS_15, e.toString(), e);
    }

  }

  private boolean leaseTableExists() {
    DescribeTableRequest request = new DescribeTableRequest();
    request.setTableName(conf.applicationName);
    DescribeTableResult result;
    try {
      result = dynamoDBClient.describeTable(request);
    } catch (ResourceNotFoundException e) {
      LOG.debug("Lease table '{}' does not exist", conf.applicationName);
      return false;
    }

    TableStatus tableStatus = TableStatus.fromValue(result.getTable().getTableStatus());
    LOG.debug("Lease table exists and is in '{}' state", tableStatus);
    return tableStatus == TableStatus.ACTIVE;
  }

  private void resetOffsets(Map<String, String> lastOffsets) {
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
        LOG.debug("Table '{}' did not exist.", tableName, e);
      }
      return false;
    }
  }
}
