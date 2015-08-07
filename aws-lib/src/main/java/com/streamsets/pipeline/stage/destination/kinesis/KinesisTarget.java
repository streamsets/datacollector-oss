/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kinesis;


import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KinesisTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisTarget.class);

  // Only Batch size is enforced. Records or batches
  // that are too large in KB will be sent to error
  // pipeline per configuration.
  public static final int MAX_BATCH_SIZE = 500;
  public static final int MAX_RECORD_SIZE_KB = 50;
  public static final int MAX_BATCH_SIZE_KB = 4500;

  /** Kinesis Configurations */
  private final Regions region;
  private final String streamName;
  private final PartitionStrategy partitionStrategy;

  /** Data Format Configurations */
  private final DataFormat dataFormat;

  private ClientConfiguration kinesisConfiguration;
  private AmazonKinesisClient kinesisClient;

  private DataGeneratorFactory generatorFactory;

  public KinesisTarget(
      final Regions region,
      final String streamName,
      final DataFormat dataFormat,
      final PartitionStrategy partitionStrategy,
      final String awsAccessKeyId,
      final String awsSecretAccessKey
  ) {
    this.region = region;
    this.streamName = streamName;
    this.dataFormat = dataFormat;
    this.partitionStrategy = partitionStrategy;

    System.setProperty("aws.accessKeyId", awsAccessKeyId);
    System.setProperty("aws.secretKey", awsSecretAccessKey);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    checkStreamExists(issues);

    if (issues.isEmpty()) {
      kinesisConfiguration = new ClientConfiguration();
      //TODO Set additional configuration options here.
      createKinesisClient();

      generatorFactory = createDataGeneratorFactory();
    }
    return issues;
  }

  private void checkStreamExists(List<ConfigIssue> issues) {
    ClientConfiguration kinesisConfiguration = new ClientConfiguration();
    AmazonKinesisClient kinesisClient = new AmazonKinesisClient(kinesisConfiguration);
    kinesisClient.setRegion(Region.getRegion(region));

    try {
      DescribeStreamResult result = kinesisClient.describeStream(streamName);
      LOG.info("Connected successfully to stream: {} with description: {}",
          streamName,
          result.getStreamDescription().toString()
      );
    } catch (Exception e) {
      issues.add(getContext().createConfigIssue(com.streamsets.pipeline.stage.origin.kinesis.Groups.KINESIS.name(),
                                                "streamName", Errors.KINESIS_01, e.toString()));
    } finally {
      kinesisClient.shutdown();
    }
  }

  @Override
  public void destroy() {
    if (kinesisClient != null) {
      kinesisClient.shutdown(); // This call is optional per Amazon docs.
    }
    super.destroy();
  }

  private void createKinesisClient() {
    kinesisClient = new AmazonKinesisClient(kinesisConfiguration);
    kinesisClient.setRegion(Region.getRegion(region));
  }

  private DataGeneratorFactory createDataGeneratorFactory() {
    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(getContext(),
        dataFormat.getGeneratorFormat());
    switch (dataFormat) {
      case SDC_JSON:
        break;
      case JSON:
        builder.setMode(JsonMode.MULTIPLE_OBJECTS);
        break;
    }
    return builder.build();
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      List<Record> records = new ArrayList<>(MAX_BATCH_SIZE);
      int numRecords = 0;
      while (numRecords < MAX_BATCH_SIZE && batchIterator.hasNext()) {
        records.add(batchIterator.next());
        ++numRecords;
      }
      processBulkPut(records);
    }
  }

  private void processBulkPut(List<Record> records) throws StageException {
    PutRecordsRequest request = new PutRecordsRequest();
    request.setStreamName(streamName);

    List<PutRecordsRequestEntry> requestEntries = new ArrayList<>();

    int i = 0;
    for (Record record : records) {
      final PutRecordsRequestEntry entry = new PutRecordsRequestEntry();

      ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024 * records.size());
      try {
        DataGenerator generator = generatorFactory.getGenerator(bytes);
        generator.write(record);
        generator.close();

        entry.setData(ByteBuffer.wrap(bytes.toByteArray()));
        entry.setPartitionKey(getPartitionKey(i));

        requestEntries.add(entry);
        ++i;
      } catch (IOException e) {
        handleFailedRecord(record, "Failed to serialize record");
      }
    }

    request.setRecords(requestEntries);
    try {
      PutRecordsResult result = kinesisClient.putRecords(request);

      final Integer failedRecordCount = result.getFailedRecordCount();
      if (failedRecordCount > 0) {
        List<PutRecordsResultEntry> resultEntries = result.getRecords();
        i = 0;
        for (PutRecordsResultEntry resultEntry : resultEntries) {
          final String errorCode = resultEntry.getErrorCode();
          if (null != errorCode) {
            switch (errorCode) {
              case "ProvisionedThroughputExceededException":
              case "InternalFailure":
                // Records are processed in the order you submit them,
                // so this will align with the initial record batch
                handleFailedRecord(records.get(i), errorCode + ":" + resultEntry.getErrorMessage());
                break;
              default:
                validateSuccessfulRecord(records.get(i), resultEntry);
                break;
            }
          } else {
            validateSuccessfulRecord(records.get(i), resultEntry);
          }
          ++i;
        }
      }
    } catch (AmazonClientException e) {
      // Unrecoverable exception -- invalidate the entire batch
      LOG.debug("Exception while putting records", e);
      for (Record record : records) {
        handleFailedRecord(record, "Batch failed due to Amazon service exception: " + e.getMessage());
      }
    }
  }

  private void handleFailedRecord(Record record, final String cause) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().toError(record, Errors.KINESIS_00, record, cause);
        break;
      case STOP_PIPELINE:
        throw new StageException(Errors.KINESIS_00, record, cause);
      default:
        throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
            getContext().getOnErrorRecord()));
    }
  }

  private String getPartitionKey(final int recordPosition) throws StageException {
    String partitionKey;

    switch (partitionStrategy) {
      case ROUND_ROBIN:
        partitionKey = String.format("partitionKey-%d", recordPosition);
        break;
      default:
        // Should never reach here.
        throw new StageException(Errors.KINESIS_02, partitionStrategy);
    }
    return partitionKey;
  }

  private void validateSuccessfulRecord(Record record, PutRecordsResultEntry resultEntry) throws StageException {
    if (null == resultEntry.getSequenceNumber() || null == resultEntry.getShardId() ||
        resultEntry.getSequenceNumber().isEmpty() || resultEntry.getShardId().isEmpty()) {
      // Some kind of other error, handle it.
      handleFailedRecord(record, "Missing SequenceId or ShardId.");
    }
  }
}
