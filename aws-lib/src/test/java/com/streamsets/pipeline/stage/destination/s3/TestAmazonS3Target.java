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
package com.streamsets.pipeline.stage.destination.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.common.FakeS3;
import com.streamsets.pipeline.stage.common.TestUtil;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.lib.aws.SSEConfigBean;
import com.streamsets.pipeline.stage.lib.aws.TransferManagerConfig;
import com.streamsets.pipeline.stage.origin.s3.S3Config;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

public class TestAmazonS3Target {

  private static final String BUCKET_NAME = "mybucket";
  private static final String DELIMITER = "/";

  private static String fakeS3Root;
  private static FakeS3 fakeS3;
  private static AmazonS3Client s3client;
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
    s3client = new AmazonS3Client(credentials);
    s3client.setEndpoint("http://localhost:" + port);
    s3client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));

    TestUtil.createBucket(s3client, BUCKET_NAME);
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
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target).build();
    targetRunner.runInit();

    List<Record> logRecords = TestUtil.createStringRecords();

    //Make sure the prefix is empty
    ObjectListing objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    Assert.assertTrue(objectListing.getObjectSummaries().isEmpty());

    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    //check that prefix contains 1 file
    objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    Assert.assertEquals(1, objectListing.getObjectSummaries().size());
    S3ObjectSummary objectSummary = objectListing.getObjectSummaries().get(0);

    //get contents of file and check data - should have 9 lines
    S3Object object = s3client.getObject(BUCKET_NAME, objectSummary.getKey());
    S3ObjectInputStream objectContent = object.getObjectContent();

    List<String> stringList = IOUtils.readLines(objectContent);
    Assert.assertEquals(9, stringList.size());
    for(int i = 0 ; i < 9; i++) {
      Assert.assertEquals(TestUtil.TEST_STRING + i, stringList.get(i));
    }
  }

  @Test
  public void testWriteTextDataWithPartitionPrefix() throws Exception {

    String prefix = "testWriteTextDataWithPartitionPrefix";
    //partition by the record id
    String partition = "${record:id()}";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, partition, false);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target).build();
    targetRunner.runInit();

    List<Record> logRecords = TestUtil.createStringRecords();

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
  public void testWriteTextDataWithCompression() throws Exception {

    String prefix = "testWriteTextDataWithCompression";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, true);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target).build();
    targetRunner.runInit();

    List<Record> logRecords = TestUtil.createStringRecords();

    //Make sure the prefix is empty
    ObjectListing objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    Assert.assertTrue(objectListing.getObjectSummaries().isEmpty());

    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    //check that prefix contains 1 file
    objectListing = s3client.listObjects(BUCKET_NAME, prefix);
    Assert.assertEquals(1, objectListing.getObjectSummaries().size());
    S3ObjectSummary objectSummary = objectListing.getObjectSummaries().get(0);

    //get contents of file and check data - should have 9 lines
    S3Object object = s3client.getObject(BUCKET_NAME, objectSummary.getKey());
    S3ObjectInputStream objectContent = object.getObjectContent();

    Assert.assertTrue(object.getKey().endsWith(".gz"));

    List<String> stringList = IOUtils.readLines(new GZIPInputStream(objectContent));
    Assert.assertEquals(9, stringList.size());
    for(int i = 0 ; i < 9; i++) {
      Assert.assertEquals(TestUtil.TEST_STRING + i, stringList.get(i));
    }
  }

  @Test
  public void testWriteEmptyBatch() throws Exception {

    String prefix = "testWriteEmptyBatch";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target).build();
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
  private AmazonS3Target createS3targetWithTextData(String commonPrefix, boolean useCompression) {
    return createS3targetWithTextData(commonPrefix, "", useCompression);
  }

  private AmazonS3Target createS3targetWithTextData(String commonPrefix, String partition, boolean useCompression) {

    S3Config s3Config = new S3Config();
    s3Config.region = AWSRegions.OTHER;
    s3Config.endpoint = "http://localhost:" + port;
    s3Config.bucket = BUCKET_NAME;
    s3Config.awsConfig = new AWSConfig();
    s3Config.awsConfig.awsAccessKeyId = "foo";
    s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3Config.commonPrefix = commonPrefix;
    s3Config.delimiter = DELIMITER;

    S3TargetConfigBean s3TargetConfigBean = new S3TargetConfigBean();
    s3TargetConfigBean.compress = useCompression;
    s3TargetConfigBean.dataFormat = DataFormat.TEXT;
    s3TargetConfigBean.partitionTemplate = partition;
    s3TargetConfigBean.fileNamePrefix = "sdc-";
    s3TargetConfigBean.timeDriverTemplate = "${time:now()}";
    s3TargetConfigBean.timeZoneID = "UTC";
    s3TargetConfigBean.s3Config = s3Config;
    s3TargetConfigBean.sseConfig = new SSEConfigBean();
    s3TargetConfigBean.proxyConfig = new ProxyConfig();
    s3TargetConfigBean.tmConfig = new TransferManagerConfig();
    s3TargetConfigBean.tmConfig.threadPoolSize = 3;

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.avroSchema = null;
    dataGeneratorFormatConfig.binaryFieldPath = "/";
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.csvFileFormat = CsvMode.CSV;
    dataGeneratorFormatConfig.csvReplaceNewLines = false;
    dataGeneratorFormatConfig.csvHeader = CsvHeader.IGNORE_HEADER;
    dataGeneratorFormatConfig.jsonMode = JsonMode.MULTIPLE_OBJECTS;
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;
    dataGeneratorFormatConfig.textFieldPath = "/";

    s3TargetConfigBean.dataGeneratorFormatConfig = dataGeneratorFormatConfig;

    return new AmazonS3Target(s3TargetConfigBean);
  }

  @Test
  public void testEventRecords() throws Exception {
    String prefix = "testEventRecords";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false);
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target).build();
    targetRunner.runInit();

    List<Record> logRecords = TestUtil.createStringRecords();

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

  @Test
  public void testEmptyPrefixForNonWholeFileFormat() throws Exception {
    String prefix = "testEventRecords";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false);
    S3TargetConfigBean targetConfigBean =  (S3TargetConfigBean) Whitebox.getInternalState(amazonS3Target, "s3TargetConfigBean");
    targetConfigBean.fileNamePrefix = "";
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target).build();

    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testEmptyPrefixForWholeFileFormat() throws Exception {
    String prefix = "testEventRecords";
    AmazonS3Target amazonS3Target = createS3targetWithTextData(prefix, false);
    S3TargetConfigBean targetConfigBean =  (S3TargetConfigBean) Whitebox.getInternalState(amazonS3Target, "s3TargetConfigBean");
    targetConfigBean.dataFormat = DataFormat.WHOLE_FILE;
    targetConfigBean.dataGeneratorFormatConfig.fileNameEL = "sample";
    targetConfigBean.fileNamePrefix = "";

    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target).build();

    List<Stage.ConfigIssue> issues = targetRunner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());
  }
}
