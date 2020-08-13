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
package com.streamsets.pipeline.stage.destination.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.sdk.service.SdkJsonDataFormatGeneratorService;
import com.streamsets.pipeline.stage.common.FakeS3;
import com.streamsets.pipeline.stage.common.TestUtil;
import com.streamsets.pipeline.stage.common.s3.AwsS3Connection;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.lib.aws.TransferManagerConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestAmazonS3Target {

  private static final String BUCKET_NAME = "mybucket";
  private static final String SECOND_BUCKET_NAME = "yourbucket";
  private static final String DELIMITER = "/";

  private static String fakeS3Root;
  private static FakeS3 fakeS3;
  private static AmazonS3 s3client;
  private static ExecutorService executorService;

  private static int port;

  @BeforeClass
  public static void setUpClass() throws IOException, InterruptedException {
    File dir = new File(new File("target", UUID.randomUUID().toString()), "fakes3_root").getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    fakeS3Root = dir.getAbsolutePath();
    port = TestUtil.getFreePort();
    fakeS3 = new FakeS3(fakeS3Root, port);
    Assume.assumeTrue("Please install fakes3 in your system", fakeS3.fakes3Installed());
    //Start the fakes3 server
    executorService = Executors.newSingleThreadExecutor();
    executorService.submit(fakeS3);

    BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");
    s3client = AmazonS3ClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:" + port, null))
        .withPathStyleAccessEnabled(true)
        .withChunkedEncodingDisabled(true) // FakeS3 does not correctly calculate checksums with chunked encoding enabled.
        .build();

    TestUtil.createBucket(s3client, BUCKET_NAME);
    TestUtil.createBucket(s3client, SECOND_BUCKET_NAME);
  }

  @AfterClass
  public static void tearDownClass() {
    if(executorService != null) {
      executorService.shutdownNow();
    }
    if(fakeS3 != null) {
      fakeS3.shutdown();
    }
  }

  @Test
  public void testWriteTextData() throws Exception {

    String prefix = "testWriteTextData";
    String suffix = "txt";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false, suffix);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target)
      .addService(DataFormatGeneratorService.class, new SdkJsonDataFormatGeneratorService())
      .build();
    targetRunner.runInit();

    List<Record> logRecords = TestUtil.createStringRecords(BUCKET_NAME);

    //Make sure the prefix is empty
    ObjectListing objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    Assert.assertTrue(objectListing.getObjectSummaries().isEmpty());

    targetRunner.runWrite(logRecords);

    TestUtil.assertStringRecords(s3client, BUCKET_NAME, prefix);

    List<EventRecord> events = targetRunner.getEventRecords();
    Assert.assertNotNull(events);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(9, events.get(0).get("/recordCount").getValueAsLong());

    targetRunner.runDestroy();
  }

  @Test
  public void testWriteToTwoBuckets() throws Exception {
    String prefix = "testWriteToTwoBuckets";
    String suffix = "txt";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false, suffix);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target)
      .addService(DataFormatGeneratorService.class, new SdkJsonDataFormatGeneratorService())
      .build();
    targetRunner.runInit();

    List<Record> logRecords = TestUtil.createStringRecords(BUCKET_NAME);
    logRecords.addAll(TestUtil.createStringRecords(SECOND_BUCKET_NAME));

    //Make sure the prefix is empty
    ObjectListing objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    Assert.assertTrue(objectListing.getObjectSummaries().isEmpty());

    objectListing = s3client.listObjects(SECOND_BUCKET_NAME, prefix);
    Assert.assertTrue(objectListing.getObjectSummaries().isEmpty());

    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    TestUtil.assertStringRecords(s3client, BUCKET_NAME, prefix);
    TestUtil.assertStringRecords(s3client, SECOND_BUCKET_NAME, prefix);
  }

  @Test
  public void testBucketResolvesToEmptyString() throws Exception {
    final String prefix = "testBucketResolvesToEmptyString";
    final String suffix = "txt";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false, suffix);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target)
      .addService(DataFormatGeneratorService.class, new SdkJsonDataFormatGeneratorService())
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    targetRunner.runInit();

    // Insert two invalid record (for which the bucket EL resolves to nothing) at the begging and end of valid data
    List<Record> logRecords = new ArrayList<>();
    logRecords.add(RecordCreator.create());
    logRecords.addAll(TestUtil.createStringRecords(BUCKET_NAME));
    logRecords.add(RecordCreator.create());

    targetRunner.runWrite(logRecords);

    // All data should be written as expected
    TestUtil.assertStringRecords(s3client, BUCKET_NAME, prefix);

    // Plus have few error records
    Assert.assertEquals(2, targetRunner.getErrorRecords().size());

    targetRunner.runDestroy();
  }

  @Test
  public void testSuffixWithPathChar() throws Exception {
    final String prefix = "testWriteTextData";
    final String suffix = ".txt";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false, suffix);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target)
      .addService(DataFormatGeneratorService.class, new SdkJsonDataFormatGeneratorService())
      .build();
    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testWriteTextDataWithPartitionPrefix() throws Exception {

    String prefix = "testWriteTextDataWithPartitionPrefix";
    String suffix = "";
    //partition by the record id
    String partition = "${record:id()}";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, partition, false, suffix);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target)
      .addService(DataFormatGeneratorService.class, new SdkJsonDataFormatGeneratorService())
      .build();
    targetRunner.runInit();

    List<Record> logRecords = TestUtil.createStringRecords(BUCKET_NAME);

    //Make sure the prefix is empty
    ObjectListing objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    Assert.assertTrue(objectListing.getObjectSummaries().isEmpty());

    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    //check that prefix contains 9 partitions
    objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
    Assert.assertEquals(9, objectSummaries.size());
    //check that full path is prefix / partition / filename
    for (int i = 0; i < objectSummaries.size(); i++) {
      Assert.assertTrue(objectSummaries.get(i).getKey().startsWith(prefix + DELIMITER + "s:" + i + DELIMITER + "sdc"));
    }
  }

  @Test
  public void testWriteEmptyBatch() throws Exception {

    String prefix = "testWriteEmptyBatch";
    String suffix = "";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false, suffix);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target)
      .addService(DataFormatGeneratorService.class, new SdkJsonDataFormatGeneratorService())
      .build();
    targetRunner.runInit();

    List<Record> logRecords = new ArrayList<>();

    //Make sure the prefix is empty
    ObjectListing objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    Assert.assertTrue(objectListing.getObjectSummaries().isEmpty());

    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    //Make sure the prefix is empty as no records were written
    objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    Assert.assertTrue(objectListing.getObjectSummaries().isEmpty());

  }

  private AmazonS3Target createS3targetWithTextData(String commonPrefix, boolean useCompression, String suffix) {
    return createS3targetWithTextData(commonPrefix, "", useCompression, suffix);
  }

  private AmazonS3Target createS3targetWithTextData(
      String commonPrefix,
      String partition,
      boolean useCompression,
      String suffix
  ) {

    S3ConnectionTargetConfig s3Config = new S3ConnectionTargetConfig();
    s3Config.connection = new AwsS3Connection();
    s3Config.connection.useRegion = true;
    s3Config.connection.region = AwsRegion.OTHER;
    s3Config.connection.endpoint = "http://localhost:" + port;
    s3Config.bucketTemplate = "${record:attribute('bucket')}";
    s3Config.connection.awsConfig = new AWSConfig();
    s3Config.connection.awsConfig.awsAccessKeyId = () -> "foo";
    s3Config.connection.awsConfig.awsSecretAccessKey = () -> "bar";
    s3Config.connection.awsConfig.disableChunkedEncoding = true;
    s3Config.commonPrefix = commonPrefix;
    s3Config.delimiter = DELIMITER;
    s3Config.connection.proxyConfig = new ProxyConfig();

    S3TargetConfigBean s3TargetConfigBean = new S3TargetConfigBean();
    s3TargetConfigBean.compress = useCompression;
    s3TargetConfigBean.partitionTemplate = partition;
    s3TargetConfigBean.fileNamePrefix = "sdc-";
    s3TargetConfigBean.timeDriverTemplate = "${time:now()}";
    s3TargetConfigBean.timeZoneID = "UTC";
    s3TargetConfigBean.s3Config = s3Config;
    s3TargetConfigBean.sseConfig = new S3TargetSSEConfigBean();
    s3TargetConfigBean.tmConfig = new TransferManagerConfig();
    s3TargetConfigBean.tmConfig.threadPoolSize = 3;
    s3TargetConfigBean.fileNameSuffix = suffix;

    return new AmazonS3Target(s3TargetConfigBean, false);
  }

  @Test
  public void testEventRecords() throws Exception {
    String prefix = "testEventRecords";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false, "");
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target)
      .addService(DataFormatGeneratorService.class, new SdkJsonDataFormatGeneratorService())
      .build();
    targetRunner.runInit();

    List<Record> logRecords = TestUtil.createStringRecords(BUCKET_NAME);

    //Make sure the prefix is empty
    ObjectListing objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    Assert.assertTrue(objectListing.getObjectSummaries().isEmpty());

    targetRunner.runWrite(logRecords);

    Assert.assertEquals(1, targetRunner.getEventRecords().size());
    Record eventRecord = targetRunner.getEventRecords().get(0);

    Assert.assertTrue(eventRecord.has("/bucket"));
    Assert.assertTrue(eventRecord.has("/objectKey"));
    Assert.assertEquals(BUCKET_NAME, eventRecord.get("/bucket").getValueAsString());
    targetRunner.runDestroy();
  }
}
