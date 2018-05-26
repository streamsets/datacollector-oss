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
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
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
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.origin.kinesis.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class KinesisUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisUtil.class);

  public static final int KB = 1024; // KiB
  public static final int ONE_MB = 1024 * KB; // MiB
  public static final String KINESIS_CONFIG_BEAN = "kinesisConfig";
  public static final String LEASE_TABLE_BEAN = "leaseTable";

  private KinesisUtil() {}

  /**
   * Checks for existence of the requested stream and adds
   * any configuration issues to the list.
   * @param conf
   * @param streamName
   * @param issues
   * @param context
   */
  public static long checkStreamExists(
      ClientConfiguration awsClientConfig,
      KinesisConfigBean conf,
      String streamName,
      List<Stage.ConfigIssue> issues,
      Stage.Context context
  ) {
    long numShards = 0;

    try {
      numShards = getShardCount(awsClientConfig, conf, streamName);
    } catch (AmazonClientException|StageException e) {
      LOG.error(Errors.KINESIS_01.getMessage(), e.toString(), e);
      issues.add(context.createConfigIssue(
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN + ".streamName", Errors.KINESIS_01, e.toString()
      ));
    }
    return numShards;
  }

  public static long getShardCount(
    ClientConfiguration awsClientConfig,
    KinesisConfigBean conf, String streamName
  ) throws StageException {
    AmazonKinesis kinesisClient = getKinesisClient(awsClientConfig, conf);

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

  private static AmazonKinesis getKinesisClient(ClientConfiguration awsClientConfig, KinesisConfigBean conf) throws StageException {

    AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder
        .standard()
        .withClientConfiguration(checkNotNull(awsClientConfig))
        .withCredentials(AWSUtil.getCredentialsProvider(conf.awsConfig));

    if (AWSRegions.OTHER == conf.region) {
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(conf.endpoint, null));
    } else {
      builder.withRegion(conf.region.getLabel());
    }

    return builder.build();
  }

  /**
   * Get the last shard Id in the given stream
   * In preview mode, kinesis source uses the last Shard Id to get records from kinesis
   * @param awsClientConfig generic AWS client configuration
   * @param conf
   * @param streamName
   */
  public static String getLastShardId(
      ClientConfiguration awsClientConfig,
      KinesisConfigBean conf,
      String streamName
  ) throws StageException {
    AmazonKinesis kinesisClient = getKinesisClient(awsClientConfig, conf);

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
      KinesisConfigBean conf,
      int maxBatchSize,
      GetShardIteratorRequest getShardIteratorRequest
  ) throws StageException {
    AmazonKinesis kinesisClient = getKinesisClient(awsClientConfig, conf);

    GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
    String shardIterator = getShardIteratorResult.getShardIterator();

    GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
    getRecordsRequest.setShardIterator(shardIterator);
    getRecordsRequest.setLimit(maxBatchSize);

    GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
    return getRecordsResult.getRecords();
  }

  public static List<com.streamsets.pipeline.api.Record> processKinesisRecord(
      String shardId,
      Record kRecord,
      DataParserFactory parserFactory
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
    return shardId + "::" + record.getPartitionKey() + "::" + record.getSequenceNumber() + "::" + ((UserRecord)
        record).getSubSequenceNumber();
  }
}
