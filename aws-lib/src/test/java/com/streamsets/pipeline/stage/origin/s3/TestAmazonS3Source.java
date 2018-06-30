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
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.sdk.service.SdkJsonDataFormatParserService;
import com.streamsets.pipeline.sdk.service.SdkWholeFileDataFormatParserService;
import com.streamsets.pipeline.stage.common.AmazonS3TestSuite;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import com.streamsets.pipeline.stage.common.TestUtil;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestAmazonS3Source extends AmazonS3TestSuite {

  private static AmazonS3 s3client;
  private static final String BUCKET_NAME = "mybucket";
  private static final String PREFIX_NAME = "myprefix";
  private static final String POSTPROCESS_BUCKET = "post-process-bucket";
  private static final String POSTPROCESS_PREFIX = "post-process-prefix";
  private static final String ERROR_BUCKET = "error-bucket";
  private static final String ERROR_PREFIX = "error-prefix";

  @BeforeClass
  public static void setUpClass() throws Exception {
    setupS3();
    populateFakes3();
  }

  @AfterClass
  public static void tearDownClass() {
    teardownS3();
  }

  private static void populateFakes3() throws IOException, InterruptedException {
    BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");
    s3client = AmazonS3ClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:" + port, null))
        .withPathStyleAccessEnabled(true)
        .withChunkedEncodingDisabled(true)
        .build();

    TestUtil.createBucket(s3client, BUCKET_NAME);
    TestUtil.createBucket(s3client, POSTPROCESS_BUCKET);
    TestUtil.createBucket(s3client, ERROR_BUCKET);

    //create directory structure
    // mybucket/NorthAmerica/USA
    // mybucket/NorthAmerica/Canada
    // mybucket/folder

    //write 3 files each under myBucket, myBucket/NorthAmerica, mybucket/NorthAmerica/USA, mybucket/NorthAmerica/Canada
    //mybuckert/folder
    //15 files in total

    InputStream in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, "file1.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "file2.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "file3.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/file4.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/file5.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/file6.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    //make sure files will have different timestamps.
    Thread.sleep(1000);

    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/USA/file7.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/USA/file8.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/USA/file9.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    //make sure files will have different timestamps.
    Thread.sleep(1000);

    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/Canada/file10.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/Canada/file11.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/Canada/file12.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    // CSV files
    in = new ByteArrayInputStream("A,B\n1,2,3\n4,5".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "csv/file0.csv", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    //Some txt files for whole file test
    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "folder/file1.txt", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "folder/file2.txt", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    in = new ByteArrayInputStream("{\"key\": \"Hello World\" }".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "folder/file3.txt", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    int count = 0;
    if(s3client.doesBucketExist(BUCKET_NAME)) {
      for(S3ObjectSummary s : S3Objects.withPrefix(s3client, BUCKET_NAME, "")) {
        System.out.println(s.getKey());
        count++;
      }
    }
    Assert.assertEquals(16, count); // 16 files + 4 dirs
  }

  @Test
  public void testProduceFullFile() throws Exception {
    try {
      AmazonS3Source source = createSource();
      SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source)
        .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
        .addOutputLane("lane")
        .build();
      runner.runInit();
      try {

        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        String offset = source.produce(null, 60000, batchMaker);
        Assert.assertNotNull(offset);
        Assert.assertTrue(offset.contains("file1.log::-1::"));

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        Assert.assertEquals(1, records.size());

        //produce records from next file
        batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 60000, batchMaker);
        Assert.assertNotNull(offset);
        Assert.assertTrue(offset.contains("file2.log::-1::"));

        output = SourceRunner.getOutput(batchMaker);
        records = output.getRecords().get("lane");
        Assert.assertEquals(1, records.size());

        //produce records from next file
        batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 60000, batchMaker);
        Assert.assertNotNull(offset);
        Assert.assertTrue(offset.contains("file3.log::-1::"));

        output = SourceRunner.getOutput(batchMaker);
        records = output.getRecords().get("lane");
        Assert.assertEquals(1, records.size());

        batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 60000, batchMaker);
        Assert.assertNotNull(offset);
        Assert.assertTrue(offset.contains("file3.log::-1::"));

        output = SourceRunner.getOutput(batchMaker);
        records = output.getRecords().get("lane");
        Assert.assertEquals(0, records.size());

      } finally {
        runner.runDestroy();
      }
    } catch (Exception e) {
      System.out.println("Hoops");
      e.printStackTrace();
      throw  e;
    }
  }

  @Test
  public void testNoMoreDataEvent() throws Exception {
    AmazonS3Source source = createSource();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source)
      .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
      .addOutputLane("lane")
      .build();
    runner.runInit();
    try {

      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      String offset = source.produce(null, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file1.log::-1::"));

      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      Assert.assertEquals(0, runner.getEventRecords().size());

      //produce records from next file
      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file2.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      Assert.assertEquals(0, runner.getEventRecords().size());

      //produce records from next file
      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file3.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      Assert.assertEquals(0, runner.getEventRecords().size());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file3.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(0, records.size());

      Assert.assertEquals(1, runner.getEventRecords().size());

      EventRecord eventRecord = runner.getEventRecords().get(0);
      Assert.assertEquals("no-more-data", eventRecord.getEventType());
      Assert.assertEquals("3", eventRecord.get("/file-count").getValueAsString());
      Assert.assertEquals("3", eventRecord.get("/record-count").getValueAsString());
      Assert.assertEquals("0", eventRecord.get("/error-count").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testLexicographicalOrdering() throws Exception {
    AmazonS3Source source = createSourceWithLexicographicalOrdering();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source)
      .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
      .addOutputLane("lane")
      .build();
    runner.runInit();
    try {
      //In lexicographical order, Canada/file*.log -> USA/file*.log -> file*.log are expected.

      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      String offset = source.produce(null, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("Canada/file10.log::-1::"));

      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("Canada/file11.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("Canada/file12.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("USA/file7.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("USA/file8.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("USA/file9.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(null, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file4.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(null, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file5.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(null, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file6.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProducePostProcessArchiveDiffBucket() throws Exception {
    AmazonS3Source source = createSourceArchiveDiffBucket();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source)
      .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
      .addOutputLane("lane")
      .build();
    runner.runInit();
    try {

      int objectsInBucket = getObjectCount(s3client, BUCKET_NAME);
      int objectsInPostProcessBucket = getObjectCount(s3client, POSTPROCESS_BUCKET);

      Assert.assertEquals(0, objectsInPostProcessBucket);

      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      String offset = source.produce(null, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file1.log::-1::"));

      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      //produce records from next file
      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file2.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      //make sure the file is moved to post processing dir
      //files get moved when source requests new object from spooler. hence the check after the second call to produce
      Assert.assertEquals(objectsInBucket - 1, getObjectCount(s3client, BUCKET_NAME));
      Assert.assertEquals(objectsInPostProcessBucket + 1, getObjectCount(s3client, POSTPROCESS_BUCKET));

      //produce records from next file
      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file3.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      //make sure the file is moved to post processing dir
      Assert.assertEquals(objectsInBucket - 2, getObjectCount(s3client, BUCKET_NAME));
      Assert.assertEquals(objectsInPostProcessBucket + 2, getObjectCount(s3client, POSTPROCESS_BUCKET));

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file3.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(0, records.size());

      Assert.assertEquals(objectsInBucket - 3, getObjectCount(s3client, BUCKET_NAME));
      Assert.assertEquals(objectsInPostProcessBucket + 3, getObjectCount(s3client, POSTPROCESS_BUCKET));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProducePostProcessArchiveDiffPrefix() throws Exception {
    AmazonS3Source source = createSourceArchiveDiffPrefix();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source)
      .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
      .addOutputLane("lane")
      .build();
    runner.runInit();
    try {

      int objectsInPrefix = getObjectCount(s3client, BUCKET_NAME, "NorthAmerica/USA");
      int objectsInPostProcessPrefix = getObjectCount(s3client, BUCKET_NAME, POSTPROCESS_PREFIX);

      Assert.assertEquals(0, objectsInPostProcessPrefix);

      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      String offset = source.produce(null, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file7.log::-1::"));

      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      //produce records from next file
      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file8.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      //make sure the file is moved to post processing dir
      //files get moved when source requests new object from spooler. hence the check after the second call to produce
      Assert.assertEquals(objectsInPrefix - 1, getObjectCount(s3client, BUCKET_NAME, "NorthAmerica/USA"));
      Assert.assertEquals(objectsInPostProcessPrefix + 1, getObjectCount(s3client, BUCKET_NAME, POSTPROCESS_PREFIX));

      //produce records from next file
      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file9.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());

      //make sure the file is moved to post processing dir
      Assert.assertEquals(objectsInPrefix - 2, getObjectCount(s3client, BUCKET_NAME, "NorthAmerica/USA"));
      Assert.assertEquals(objectsInPostProcessPrefix + 2, getObjectCount(s3client, BUCKET_NAME, POSTPROCESS_PREFIX));

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(offset, 60000, batchMaker);
      Assert.assertNotNull(offset);
      Assert.assertTrue(offset.contains("file9.log::-1::"));

      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertEquals(0, records.size());

      Assert.assertEquals(objectsInPrefix - 3, getObjectCount(s3client, BUCKET_NAME, "NorthAmerica/USA"));
      Assert.assertEquals(objectsInPostProcessPrefix + 3, getObjectCount(s3client, BUCKET_NAME, POSTPROCESS_PREFIX));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidConfig() throws StageException {
    AmazonS3Source source = createSourceWithSameBucketsAndPrefix();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source)
      .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
      .addOutputLane("lane")
      .build();
    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    Assert.assertEquals(2, configIssues.size());
  }

  @Test
  public void testInvalidErrorHandlingConfig() throws StageException {
    AmazonS3Source source = createSourceWithWrongErrorHandlingPostProcessing();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source)
      .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
      .addOutputLane("lane")
      .build();
    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());
  }

  @Test
  public void testValidConfig1() throws StageException {
    AmazonS3Source source = createSourceWithSameBucketDiffPrefix();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source)
      .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
      .addOutputLane("lane")
      .build();
    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    Assert.assertEquals(0, configIssues.size());
  }

  @Test
  public void testValidConfig2() throws StageException {
    AmazonS3Source source = createSourceWithDiffBucketSamePrefix();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source)
      .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
      .addOutputLane("lane")
      .build();
    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    Assert.assertEquals(0, configIssues.size());
  }

  @Test
  public void testNormalizePrefix() {
    String prefix = "/";
    String delimiter = "/";
    prefix = AWSUtil.normalizePrefix(prefix, delimiter);
    Assert.assertEquals("", prefix);

    prefix = "/foo";
    delimiter = "/";
    prefix = AWSUtil.normalizePrefix(prefix, delimiter);
    Assert.assertEquals("foo/", prefix);
  }

  @Test
  public void testWholeFile() throws Exception {
    AmazonS3Source source = createSourceWithWholeFile();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source)
      .addService(DataFormatParserService.class, new SdkWholeFileDataFormatParserService())
      .addOutputLane("lane")
      .build();
    runner.runInit();
    String lastOffset = null;
    try {
      Map<Pair<String, String>, S3ObjectSummary> s3ObjectSummaryMap = getObjectSummaries(s3client, BUCKET_NAME, "folder");
      for (int i = 0; i < s3ObjectSummaryMap.size(); i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        lastOffset = source.produce(lastOffset, 1000, batchMaker);
        Assert.assertNotNull(lastOffset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        Assert.assertEquals(1, records.size());

        Record record = records.get(0);
        Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH));
        Assert.assertTrue(record.has(FileRefUtil.FILE_REF_FIELD_PATH));

        Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/bucket"));
        Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/objectKey"));

        String objectKey = record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/objectKey").getValueAsString();

        S3ObjectSummary objectSummary = s3ObjectSummaryMap.get(Pair.of(BUCKET_NAME, objectKey));

        Record.Header header = record.getHeader();

        Map<String, Object> metadata = AmazonS3Util.getMetaData(
            AmazonS3Util.getObject(
                s3client,
                objectSummary.getBucketName(),
                objectSummary.getKey(),
                false,
                () -> "",
                () -> ""
            )
        );

        for (Map.Entry<String, Object> metadataEntry : metadata.entrySet()) {
          if (!metadataEntry.getKey().equals("Content-Length")) {
            Assert.assertNotNull(header.getAttribute(metadataEntry.getKey()));
            String value = metadataEntry.getValue() != null ? metadataEntry.getValue().toString() : "";
            Assert.assertEquals("Mismatch in header: " + metadataEntry.getKey(), value, header.getAttribute(metadataEntry.getKey()));

            Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + metadataEntry.getKey()));
            Field expectedValue = FileRefUtil.createFieldForMetadata(metadataEntry.getValue());
            Assert.assertEquals("Mismatch in Field: " + metadataEntry.getKey(), expectedValue.getValue(), record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + metadataEntry.getKey()).getValue());
          }
        }
        Assert.assertEquals(objectSummary.getKey(), header.getAttribute("Name"));
        Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/size"));
        Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/filename"));
        Assert.assertEquals(objectSummary.getSize(), record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/size").getValueAsLong());
        Assert.assertEquals(objectSummary.getKey(), record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.FILE_NAME).getValueAsString());
      }
    } finally {
      runner.runDestroy();
    }
  }

  private AmazonS3Source createSource() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.sseConfig = new S3SSEConfigBean();
    s3ConfigBean.sseConfig.useCustomerSSEKey = false;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;
    s3ConfigBean.errorConfig.errorPrefix = ERROR_PREFIX;
    s3ConfigBean.errorConfig.errorBucket = ERROR_BUCKET;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;
    s3ConfigBean.postProcessingConfig.postProcessBucket = POSTPROCESS_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessPrefix = POSTPROCESS_PREFIX;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.log";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.s3Config = new S3ConnectionSourceConfig();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = () -> "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = () -> "bar";
    s3ConfigBean.s3Config.awsConfig.disableChunkedEncoding = true;
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceWithLexicographicalOrdering() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.sseConfig = new S3SSEConfigBean();
    s3ConfigBean.sseConfig.useCustomerSSEKey = false;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;
    s3ConfigBean.errorConfig.errorPrefix = ERROR_PREFIX;
    s3ConfigBean.errorConfig.errorBucket = ERROR_BUCKET;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;
    s3ConfigBean.postProcessingConfig.postProcessBucket = POSTPROCESS_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessPrefix = POSTPROCESS_PREFIX;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "**/*.log";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.LEXICOGRAPHICAL;

    s3ConfigBean.s3Config = new S3ConnectionSourceConfig();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = () -> "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = () -> "bar";
    s3ConfigBean.s3Config.awsConfig.disableChunkedEncoding = true;
    s3ConfigBean.s3Config.commonPrefix = "NorthAmerica";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceArchiveDiffBucket() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.sseConfig = new S3SSEConfigBean();
    s3ConfigBean.sseConfig.useCustomerSSEKey = false;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.errorConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.errorConfig.errorPrefix = ERROR_PREFIX;
    s3ConfigBean.errorConfig.errorBucket = ERROR_BUCKET;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.postProcessingConfig.postProcessBucket = POSTPROCESS_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessPrefix = POSTPROCESS_PREFIX;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.log";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.s3Config = new S3ConnectionSourceConfig();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = () -> "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = () -> "bar";
    s3ConfigBean.s3Config.awsConfig.disableChunkedEncoding = true;
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceArchiveDiffPrefix() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.sseConfig = new S3SSEConfigBean();
    s3ConfigBean.sseConfig.useCustomerSSEKey = false;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.errorConfig.archivingOption =  S3ArchivingOption.MOVE_TO_PREFIX;
    s3ConfigBean.errorConfig.errorPrefix = ERROR_PREFIX;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.postProcessingConfig.postProcessBucket = BUCKET_NAME;
    s3ConfigBean.postProcessingConfig.postProcessPrefix = POSTPROCESS_PREFIX;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.log";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.s3Config = new S3ConnectionSourceConfig();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = () -> "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = () -> "bar";
    s3ConfigBean.s3Config.awsConfig.disableChunkedEncoding = true;
    s3ConfigBean.s3Config.commonPrefix = "NorthAmerica/USA/";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceWithSameBucketsAndPrefix() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.sseConfig = new S3SSEConfigBean();
    s3ConfigBean.sseConfig.useCustomerSSEKey = false;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.errorConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.errorConfig.errorPrefix = PREFIX_NAME;
    s3ConfigBean.errorConfig.errorBucket = BUCKET_NAME;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessBucket = BUCKET_NAME;
    s3ConfigBean.postProcessingConfig.postProcessPrefix = PREFIX_NAME;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.log";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.s3Config = new S3ConnectionSourceConfig();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = () -> "AKIAJ6S5Q43F4BT6ZJLQ";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = () -> "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";
    s3ConfigBean.s3Config.awsConfig.disableChunkedEncoding = true;
    s3ConfigBean.s3Config.commonPrefix = PREFIX_NAME;
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceWithSameBucketDiffPrefix() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.sseConfig = new S3SSEConfigBean();
    s3ConfigBean.sseConfig.useCustomerSSEKey = false;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.errorConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.errorConfig.errorPrefix = "test-error-prefix1/";
    s3ConfigBean.errorConfig.errorBucket = BUCKET_NAME;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.postProcessingConfig.postProcessBucket = BUCKET_NAME;
    s3ConfigBean.postProcessingConfig.postProcessPrefix = "test-error-prefix2/";

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.log";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.s3Config = new S3ConnectionSourceConfig();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = () -> "AKIAJ6S5Q43F4BT6ZJLQ";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = () -> "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";
    s3ConfigBean.s3Config.awsConfig.disableChunkedEncoding = true;
    s3ConfigBean.s3Config.commonPrefix = "test-error-prefix3/";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceWithDiffBucketSamePrefix() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.sseConfig = new S3SSEConfigBean();
    s3ConfigBean.sseConfig.useCustomerSSEKey = false;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.errorConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.errorConfig.errorPrefix = PREFIX_NAME;
    s3ConfigBean.errorConfig.errorBucket = ERROR_BUCKET;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.postProcessingConfig.postProcessBucket = POSTPROCESS_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessPrefix = PREFIX_NAME;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.log";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.s3Config = new S3ConnectionSourceConfig();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = () -> "AKIAJ6S5Q43F4BT6ZJLQ";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = () -> "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";
    s3ConfigBean.s3Config.awsConfig.disableChunkedEncoding = true;
    s3ConfigBean.s3Config.commonPrefix = PREFIX_NAME;
    s3ConfigBean.s3Config.delimiter = "/";

    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceWithWrongErrorHandlingPostProcessing() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.sseConfig = new S3SSEConfigBean();
    s3ConfigBean.sseConfig.useCustomerSSEKey = false;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessBucket = BUCKET_NAME;
    s3ConfigBean.postProcessingConfig.postProcessPrefix = POSTPROCESS_PREFIX;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.log";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.s3Config = new S3ConnectionSourceConfig();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = () -> "AKIAJ6S5Q43F4BT6ZJLQ";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = () -> "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";
    s3ConfigBean.s3Config.awsConfig.disableChunkedEncoding = true;
    s3ConfigBean.s3Config.commonPrefix = PREFIX_NAME;
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }


  private AmazonS3Source createSourceWithWholeFile() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.sseConfig = new S3SSEConfigBean();
    s3ConfigBean.sseConfig.useCustomerSSEKey = false;
    //include metadata in header.
    s3ConfigBean.enableMetaData = true;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.txt";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.s3Config = new S3ConnectionSourceConfig();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = () -> "AKIAJ6S5Q43F4BT6ZJLQ";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = () -> "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";
    s3ConfigBean.s3Config.awsConfig.disableChunkedEncoding = true;
    s3ConfigBean.s3Config.commonPrefix = "folder";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private int getObjectCount(AmazonS3 s3Client, String bucket) {
    int count = 0;
    for(S3ObjectSummary ignored : S3Objects.inBucket(s3Client, bucket)) {
      count++;
    }
    return count;
  }

  private Map<Pair<String, String>, S3ObjectSummary> getObjectSummaries(AmazonS3 s3Client, String bucket, String prefix) {
    Map<Pair<String, String>, S3ObjectSummary> s3ObjectSummaries = new HashMap<>();
    for(S3ObjectSummary s : S3Objects.withPrefix(s3Client, bucket, prefix)) {
      s3ObjectSummaries.put(Pair.of(bucket, s.getKey()), s);
    }
    return s3ObjectSummaries;
  }

  private int getObjectCount(AmazonS3 s3Client, String bucket, String prefix) {
    int count = 0;
    for(S3ObjectSummary ignored : S3Objects.withPrefix(s3Client, bucket, prefix)) {
      count++;
    }
    return count;
  }
}
