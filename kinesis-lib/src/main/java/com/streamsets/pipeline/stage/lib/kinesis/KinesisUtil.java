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
package com.streamsets.pipeline.stage.lib.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.lib.aws.AWSKinesisUtil;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.stage.origin.kinesis.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class KinesisUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisUtil.class);

  public static final int KB = 1024; // KiB
  public static final int ONE_MB = 1024 * KB; // MiB
  private static final int MILLIS = 1000;
  public static final String KINESIS_CONFIG_BEAN = "kinesisConfig";
  public static final String KINESIS_CONFIG_BEAN_CONNECTION = KINESIS_CONFIG_BEAN + ".connection";
  public static final String LEASE_TABLE_BEAN = "leaseTable";

  public static final Pattern REGION_PATTERN = Pattern.compile(
      "(?:https?://)?[\\w-]*\\.?kinesis\\.([\\w-]+)(?:\\.vpce)?\\.amazonaws\\.com");

  private KinesisUtil() {}

  /**
   * Checks for existence of the requested stream and adds
   * any configuration issues to the list.
   * @param kinesisConnection
   * @param streamName
   * @param issues
   * @param context
   */
  public static long checkStreamExists(
      ClientConfiguration awsClientConfig,
      AwsKinesisStreamConnection kinesisConnection,
      String streamName,
      List<Stage.ConfigIssue> issues,
      Stage.Context context
  ) {
    long numShards = 0;

    try {
      numShards = getShardCount(awsClientConfig, kinesisConnection, streamName, context);
    } catch (AmazonClientException|StageException e) {
      LOG.error(Errors.KINESIS_01.getMessage(), e.toString(), e);
      issues.add(context.createConfigIssue(Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN + ".streamName",
          Errors.KINESIS_01,
          e.toString()
      ));
    }
    return numShards;
  }

  public static long getShardCount(
      ClientConfiguration awsClientConfig, AwsKinesisStreamConnection conn, String streamName, Stage.Context context
  ) {
    AmazonKinesis kinesisClient = getKinesisClient(awsClientConfig, conn, context);

    try {
      long numShards = 0;
      String lastShardId = null;
      StreamDescription description;
      do {
        if (lastShardId == null) {
          description = kinesisClient.describeStream(streamName).getStreamDescription();
        } else {
          description = kinesisClient.describeStream(streamName, lastShardId).getStreamDescription();
        }

        for (Shard shard : description.getShards()) {
          if (shard.getSequenceNumberRange().getEndingSequenceNumber() == null) {
            // Then this shard is open, so we should count it. Shards with an ending sequence number
            // are closed and cannot be written to, so we skip counting them.
            ++numShards;
          }
        }

        int pageSize = description.getShards().size();
        lastShardId = description.getShards().get(pageSize - 1).getShardId();

      } while (description.getHasMoreShards());

      LOG.debug("Connected successfully to stream: '{}' with '{}' shards.", streamName, numShards);

      return numShards;
    } finally {
      kinesisClient.shutdown();
    }
  }

  private static AmazonKinesis getKinesisClient(ClientConfiguration awsClientConfig, KinesisStreamConfigBean conf, Stage.Context context) {
    return getKinesisClient(awsClientConfig, conf.connection, context);
  }

  private static AmazonKinesis getKinesisClient(
      ClientConfiguration awsClientConfig, AwsKinesisStreamConnection connection, Stage.Context context
  ) {
    AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder.standard().withClientConfiguration(checkNotNull(
        awsClientConfig));

    Regions region = Regions.DEFAULT_REGION;
    if (AwsRegion.OTHER == connection.region) {
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(connection.endpoint, null));
    } else {
      region = Regions.fromName(connection.region.getId().toLowerCase());
      builder.withRegion(region);
    }

    return builder.withCredentials(AWSKinesisUtil.getCredentialsProvider(connection.awsConfig, context, region)).build();
  }

  /**
   * Get the last shard Id in the given stream
   * In preview mode, kinesis source uses the last Shard Id to get records from kinesis
   *
   * @param awsClientConfig generic AWS client configuration
   * @param conf
   * @param streamName
   */
  public static String getLastShardId(
      ClientConfiguration awsClientConfig,
      KinesisStreamConfigBean conf,
      String streamName,
      Stage.Context context
  ) {
    AmazonKinesis kinesisClient = getKinesisClient(awsClientConfig, conf, context);

    String lastShardId = null;
    try {
      StreamDescription description;
      do {
        if (lastShardId == null) {
          description = kinesisClient.describeStream(streamName).getStreamDescription();
        } else {
          description = kinesisClient.describeStream(streamName, lastShardId).getStreamDescription();
        }

        int pageSize = description.getShards().size();
        lastShardId = description.getShards().get(pageSize - 1).getShardId();

      } while (description.getHasMoreShards());

      return lastShardId;

    } finally {
      kinesisClient.shutdown();
    }
  }

  public static List<com.amazonaws.services.kinesis.model.Record> getPreviewRecords(
      ClientConfiguration awsClientConfig,
      KinesisStreamConfigBean conf,
      int maxBatchSize,
      GetShardIteratorRequest getShardIteratorRequest,
      Stage.Context context
  ) {
    AmazonKinesis kinesisClient = getKinesisClient(awsClientConfig, conf, context);

    GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
    String shardIterator = getShardIteratorResult.getShardIterator();

    GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
    getRecordsRequest.setShardIterator(shardIterator);
    getRecordsRequest.setLimit(maxBatchSize);

    GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
    return getRecordsResult.getRecords();
  }

  public static List<com.streamsets.pipeline.api.Record> processKinesisRecord(
      String shardId, Record kRecord, DataParserFactory parserFactory
  ) throws DataParserException, IOException {
    final String recordId = createKinesisRecordId(shardId, kRecord);
    DataParser parser = parserFactory.getParser(recordId, kRecord.getData().array());

    List<com.streamsets.pipeline.api.Record> records = new ArrayList<>();
    com.streamsets.pipeline.api.Record r;
    while ((r = parser.parse()) != null) {
      records.add(r);
    }
    parser.close();
    return records;
  }

  public static String createKinesisRecordId(String shardId, com.amazonaws.services.kinesis.model.Record record) {
    return shardId + "::" + record.getPartitionKey() + "::" + record.getSequenceNumber() + "::" + (
        (UserRecord) record
    ).getSubSequenceNumber();
  }

  public static void addAdditionalKinesisClientConfiguration(
      KinesisClientLibConfiguration conf, Map<String, String> additionalConfiguration, List<Stage.ConfigIssue> issues,
      Stage.Context context
  ) {
    for (Map.Entry<String, String> property : additionalConfiguration.entrySet()) {
      try {
        switch (AdditionalClientConfiguration.getName(property.getKey())) {
          case FAIL_OVER_TIME_MILLIS:
            conf.withFailoverTimeMillis(Long.parseLong(property.getValue()));
            break;
          case TASK_BACKOFF_TIME_MILLIS:
            conf.withTaskBackoffTimeMillis(Long.parseLong(property.getValue()));
            break;
          case METRICS_BUFFER_TIME_MILLIS:
            conf.withMetricsBufferTimeMillis(Long.parseLong(property.getValue()));
            break;
          case METRICS_MAX_QUEUE_SIZE:
            conf.withMetricsMaxQueueSize(Integer.parseInt(property.getValue()));
            break;
          case VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECK_POINTING:
            conf.withValidateSequenceNumberBeforeCheckpointing(Boolean.parseBoolean(property.getValue()));
            break;
          case SHUTDOWN_GRACE_MILLIS:
            conf.withShutdownGraceMillis(Long.parseLong(property.getValue()));
            break;
          case BILLING_MODE:
            BillingMode billingMode = BillingMode.fromValue(property.getValue());
            conf.withBillingMode(billingMode);
            break;
          case TIME_OUT_IN_SECONDS:
            conf.withTimeoutInSeconds(Integer.parseInt(property.getValue()));
            break;
          case RETRY_GET_RECORDS_IN_SECONDS:
            conf.withRetryGetRecordsInSeconds(Integer.parseInt(property.getValue()));
            break;
          case MAX_GET_RECORDS_THREAD_POOL:
            conf.withMaxGetRecordsThreadPool(Integer.parseInt(property.getValue()));
            break;
          case MAX_LEASE_RENEWAL_THREADS:
            conf.withMaxLeaseRenewalThreads(Integer.parseInt(property.getValue()));
            break;
          case LOG_WARNING_FOR_TASK_AFTER_MILLIS:
            conf.withLogWarningForTaskAfterMillis(Long.parseLong(property.getValue()));
            break;
          case LIST_SHARDS_BACK_OFF_TIME_IN_MILLIS:
            conf.withListShardsBackoffTimeInMillis(Long.parseLong(property.getValue()));
            break;
          case MAX_LIST_SHARDS_RETRY_ATTEMPTS:
            conf.withMaxListShardsRetryAttempts(Integer.parseInt(property.getValue()));
            break;
          case CLEAN_UP_LEASES_UPON_SHARD_COMPLETION:
            conf.withCleanupLeasesUponShardCompletion(Boolean.parseBoolean(property.getValue()));
            break;
          default:
            LOG.error(Errors.KINESIS_21.getMessage(), property.getKey());
            break;
        }
      } catch (IllegalArgumentException ex) {
        LOG.error(Utils.format(Errors.KINESIS_25.getMessage(), ex.toString()), ex);
        issues.add(context.createConfigIssue(Groups.KINESIS.name(),
            KINESIS_CONFIG_BEAN + ".kinesisConsumerConfigs",
            Errors.KINESIS_25,
            ex.toString()
        ));
      }
    }
  }
}
