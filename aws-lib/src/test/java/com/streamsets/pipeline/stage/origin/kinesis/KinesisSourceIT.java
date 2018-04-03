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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.awaitility.Duration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.amazonaws.SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Ignore
public class KinesisSourceIT {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisSourceIT.class);
  private static final int DYNAMO_DB_PORT = 8000;
  private static final int KINESIS_PORT = 4567;
  private static final String SINGLE_SHARD_STREAM = "SINGLE_SHARD_STREAM";
  private static final String MULTI_SHARD_STREAM = "MULTI_SHARD_STREAM";
  private static final String BAD_RECORD_STREAM = "BAD_RECORD_STREAM";

  @Parameters
  public static Collection<Object[]> streams() {
    // streamName, numRecords, numErrorRecords
    return Arrays.asList(new Object[][]{
        {SINGLE_SHARD_STREAM, 10, 0}, {MULTI_SHARD_STREAM, 20, 0}, {BAD_RECORD_STREAM, 11, 1}
    });
  }

  @Parameter
  public String streamName;

  @Parameter(1)
  public int numRecords;

  @Parameter(2)
  public int numErrorRecords;

  @ClassRule
  public static GenericContainer dynamo = new GenericContainer(
      new ImageFromDockerfile()
      .withDockerfileFromBuilder(builder ->{
        builder
            .from("java:8")
            .run("curl -sL http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz | " +
                "tar xz && rm -rf LICENSE.txt README.txt third_party_licenses/")
            .expose(DYNAMO_DB_PORT)
            .entryPoint("java", "-Djava.library.path=./DynamoDBLocal_lib", "-jar", "DynamoDBLocal.jar", "-sharedDb")
            .build();
      })
  ).withExposedPorts(DYNAMO_DB_PORT);

  @ClassRule
  public static GenericContainer kinesalite = new GenericContainer("instructure/kinesalite:latest")
      .withExposedPorts(KINESIS_PORT);

  @BeforeClass
  public static void setUpClass() throws Exception {
    AmazonKinesis client = AmazonKinesisClientBuilder
        .standard()
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(getKinesisEndpoint(), null))
        .build();

    System.setProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");

    client.createStream(SINGLE_SHARD_STREAM, 1);
    client.createStream(MULTI_SHARD_STREAM, 2);
    client.createStream(BAD_RECORD_STREAM, 1);
    streams().forEach(s -> await().atMost(15, SECONDS).until(() -> streamActive(client, (String)s[0])));

    PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
    putRecordsRequest.setStreamName(SINGLE_SHARD_STREAM);
    putRecordsRequest.setRecords(getRecords(10));
    PutRecordsResult putResult = client.putRecords(putRecordsRequest);
    assertEquals((Integer) 0, putResult.getFailedRecordCount());

    putRecordsRequest = new PutRecordsRequest();
    putRecordsRequest.setStreamName(MULTI_SHARD_STREAM);
    putRecordsRequest.setRecords(getRecords(20));
    putResult = client.putRecords(putRecordsRequest);
    assertEquals((Integer) 0, putResult.getFailedRecordCount());

    putRecordsRequest = new PutRecordsRequest();
    putRecordsRequest.setStreamName(BAD_RECORD_STREAM);
    Collection<PutRecordsRequestEntry> badRecordStream = getRecords(4);
    badRecordStream.add(getBadRecord());
    badRecordStream.addAll(getRecords(7));
    putRecordsRequest.setRecords(badRecordStream);
    putResult = client.putRecords(putRecordsRequest);
    assertEquals((Integer) 0, putResult.getFailedRecordCount());
  }

  private static Collection<PutRecordsRequestEntry> getRecords(int numRecords) {
    return IntStream.range(0, numRecords).mapToObj(i -> {
      PutRecordsRequestEntry record = new PutRecordsRequestEntry();
      record.setData(ByteBuffer.wrap("{\"a\":1,\"b\":2,\"c\":2}".getBytes(Charsets.UTF_8)));
      record.setPartitionKey(String.valueOf(i));
      return record;
    }).collect(Collectors.toList());
  }

  private static PutRecordsRequestEntry getBadRecord() {
    PutRecordsRequestEntry badRecord = new PutRecordsRequestEntry();
    badRecord.setData(ByteBuffer.wrap("{\"a\":1,\"b\":2,\"c\":2,".getBytes(Charsets.UTF_8)));
    badRecord.setPartitionKey(String.valueOf(0));
    return badRecord;
  }

  private static boolean streamActive(AmazonKinesis client, String streamName) {
    try {
      DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
      describeStreamRequest.setStreamName(streamName);
      DescribeStreamResult describeStreamResult = client.describeStream(describeStreamRequest);
      String streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
      if ("ACTIVE".equals(streamStatus)) {
        return true;
      }
    } catch (Exception e) {
      return false;
    }
    return false;
  }

  @Test
  public void testConsume() throws Exception {
    KinesisConsumerConfigBean config = getKinesisConsumerConfig(streamName);
    final Map<String, String> lastSourceOffsets = new HashMap<>();

    for (int i = 0; i < 2; i++) {
      KinesisSource kinesisSource = new KinesisSource(config);
      PushSourceRunner runner = new PushSourceRunner.Builder(KinesisDSource.class, kinesisSource).addOutputLane("lane")
          .setOnRecordError(OnRecordError.TO_ERROR)
          .build();

      runner.runInit();
      kinesisSource.setDynamoDBClient(getDynamoDBClient());
      kinesisSource.setMetricsFactory(new NullMetricsFactory());

      final List<Record> records = new ArrayList<>(numRecords);
      final AtomicBoolean isDone = new AtomicBoolean(false);

      try {
        runner.runProduce(lastSourceOffsets, 5, output -> {
          records.addAll(output.getRecords().get("lane"));
          if (records.size() == numRecords) {
            isDone.set(true);
            runner.setStop();
          }
        });

        // This stage doesn't produce empty batches, so timeout the test
        // if it doesn't run to completion in a reasonable amount of time.
        await().atMost(Duration.TWO_MINUTES).untilTrue(isDone);
        runner.waitOnProduce();
        assertEquals(numErrorRecords, runner.getErrorRecords().size());
      } finally {
        runner.runDestroy();
      }

      assertEquals(numRecords, records.size());
    }
  }

  @Test
  public void testPreview() throws Exception {
    KinesisConsumerConfigBean config = getKinesisConsumerConfig(SINGLE_SHARD_STREAM);

    KinesisSource kinesisSource = new KinesisSource(config);
    PushSourceRunner runner = new PushSourceRunner.Builder(KinesisDSource.class, kinesisSource)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.DISCARD)
        .setPreview(true)
        .build();

    runner.runInit();
    kinesisSource.setDynamoDBClient(getDynamoDBClient());
    kinesisSource.setMetricsFactory(new NullMetricsFactory());

    final List<Record> records = new ArrayList<>(numRecords);
    final AtomicInteger batchCount = new AtomicInteger(0);
    try {
      runner.runProduce(new HashMap<>(), 10, output -> {
        batchCount.incrementAndGet();
        records.addAll(output.getRecords().get("lane"));
      });

      runner.waitOnProduce();
      assertEquals(1, batchCount.get());
      assertEquals(10, records.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStopOnError() throws Exception {
    if (!BAD_RECORD_STREAM.equals(streamName)) {
      // Skip the streams without bad records, simpler than moving
      // one or two tests into their own class.
      return;
    }

    KinesisConsumerConfigBean config = getKinesisConsumerConfig(streamName);

    KinesisSource kinesisSource = new KinesisSource(config);
    PushSourceRunner runner = new PushSourceRunner.Builder(KinesisDSource.class, kinesisSource)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    runner.runInit();
    kinesisSource.setDynamoDBClient(getDynamoDBClient());
    kinesisSource.setMetricsFactory(new NullMetricsFactory());

    final List<Record> records = new ArrayList<>(numRecords);
    final AtomicInteger batchCount = new AtomicInteger(0);
    try {
      runner.runProduce(new HashMap<>(), 10, output -> {
        batchCount.incrementAndGet();
        records.addAll(output.getRecords().get("lane"));
      });

      runner.waitOnProduce(); // Should finish without setStop due to error
      assertEquals(0, runner.getErrorRecords().size());
    } catch (Exception e) {
      List<Throwable> stageExceptions = Throwables.getCausalChain(e)
          .stream()
          .filter(t -> StageException.class.isAssignableFrom(t.getClass()))
          .collect(Collectors.toList());
      assertEquals(1, stageExceptions.size());
      assertEquals(Errors.KINESIS_03, ((OnRecordErrorException) stageExceptions.get(0)).getErrorCode());
    } finally {
      runner.runDestroy();
    }

    assertEquals(1, batchCount.get());
    assertEquals(4, records.size());
  }

  private KinesisConsumerConfigBean getKinesisConsumerConfig(String streamName) {
    KinesisConsumerConfigBean conf = new KinesisConsumerConfigBean();
    conf.dataFormatConfig = new DataParserFormatConfig();
    conf.awsConfig = new AWSConfig();
    conf.awsConfig.awsAccessKeyId = () -> "foo";
    conf.awsConfig.awsSecretAccessKey = () -> "boo";

    conf.region = AWSRegions.OTHER;
    conf.endpoint = getKinesisEndpoint();
    conf.streamName = streamName;

    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.jsonMaxObjectLen = 1024;

    conf.applicationName = UUID.randomUUID().toString();
    conf.idleTimeBetweenReads = 250;
    conf.initialPositionInStream = InitialPositionInStream.TRIM_HORIZON;
    conf.maxBatchSize = 1000;
    conf.maxRecordProcessors = 2; // Must be at least 1

    return conf;
  }

  private static String getKinesisEndpoint() {
    return "http://" + kinesalite.getContainerIpAddress() + ":" + kinesalite.getMappedPort(KINESIS_PORT);
  }

  private static String getDynamoEndpoint() {
    return "http://" + dynamo.getContainerIpAddress() + ":" + dynamo.getMappedPort(DYNAMO_DB_PORT);
  }

  private AmazonDynamoDB getDynamoDBClient() {
    return AmazonDynamoDBClientBuilder
        .standard()
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(getDynamoEndpoint(), null))
        .build();
  }
}
