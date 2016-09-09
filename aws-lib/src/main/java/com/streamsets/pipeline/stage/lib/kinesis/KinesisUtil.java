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
package com.streamsets.pipeline.stage.lib.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.origin.kinesis.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KinesisUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisUtil.class);
  private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
  private static final int NUM_RETRIES = 10;

  public static final int KB = 1024; // KiB
  public static final int ONE_MB = 1024 * KB; // MiB
  public static final String KINESIS_CONFIG_BEAN = "kinesisConfig";

  private KinesisUtil() {}

  /**
   * Checks for existence of the requested stream and adds
   * any configuration issues to the list.
   * @param regionName
   * @param streamName
   * @param issues
   * @param context
   */
  public static long checkStreamExists(
      String regionName,
      String streamName,
      AWSConfig awsConfig,
      List<Stage.ConfigIssue> issues,
      Stage.Context context
  ) {
    long numShards = 0;

    try {
      numShards = getShardCount(regionName, awsConfig, streamName);
    } catch (AmazonClientException e) {
      issues.add(context.createConfigIssue(
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN + ".streamName", Errors.KINESIS_01, e.toString()
      ));
    }
    return numShards;
  }

  public static long getShardCount(String regionName, AWSConfig awsConfig, String streamName)
    throws AmazonClientException {
    ClientConfiguration kinesisConfiguration = new ClientConfiguration();
    AmazonKinesisClient kinesisClient = new AmazonKinesisClient(
        AWSUtil.getCredentialsProvider(awsConfig),
        kinesisConfiguration
    );
    kinesisClient.setRegion(RegionUtils.getRegion(regionName));

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

  public static void checkpoint(IRecordProcessorCheckpointer checkpointer) {
    checkpoint(checkpointer, null, null);
  }

  public static void checkpoint(IRecordProcessorCheckpointer checkpointer, String sequenceNumber) {
    checkpoint(checkpointer, sequenceNumber, null);
  }

  public static void checkpoint(
      IRecordProcessorCheckpointer checkpointer,
      String sequenceNumber,
      Long subsequenceNumber
  ) {
    for (int i = 0; i < NUM_RETRIES; i++) {
      try {
        if (subsequenceNumber != null && sequenceNumber != null) {
          checkpointer.checkpoint(sequenceNumber, subsequenceNumber);
        } else if (sequenceNumber != null) {
          checkpointer.checkpoint(sequenceNumber);
        } else {
          checkpointer.checkpoint();
        }
        break;
      } catch (ShutdownException se) {
        // Ignore checkpoint if the processor instance has been shutdown (fail over).
        LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        break;
      } catch (ThrottlingException e) {
        // Backoff and re-attempt checkpoint upon transient failures
        if (i >= (NUM_RETRIES - 1)) {
          LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
          break;
        } else {
          LOG.warn("Transient issue when checkpointing - attempt {} of {}", (i + 1), NUM_RETRIES, e);
        }
      } catch (InvalidStateException e) {
        // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
        LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        break;
      }

      try {
        Thread.sleep(BACKOFF_TIME_IN_MILLIS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while retrying checkpoint.", e);
      }
    }
  }
}
