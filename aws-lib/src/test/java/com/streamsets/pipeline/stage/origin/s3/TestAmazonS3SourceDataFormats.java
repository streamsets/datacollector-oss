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
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.FakeS3;
import com.streamsets.pipeline.stage.common.TestUtil;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestAmazonS3SourceDataFormats {

  private static String fakeS3Root;
  private static ExecutorService executorService;
  private static FakeS3 fakeS3;
  private static AmazonS3Client s3client;
  private static final String BUCKET_NAME = "testamazonssourcedataformats";
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

    populateFakes3();
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

  private static void populateFakes3() throws IOException, InterruptedException {
    BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");
    s3client = new AmazonS3Client(credentials);
    s3client.setEndpoint("http://localhost:" + port);
    s3client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));

    TestUtil.createBucket(s3client, BUCKET_NAME);

    //write files each under myBucket
    //delimited
    InputStream in = Resources.getResource("sample_csv.csv").openStream();
    PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, "sample_csv.csv", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    //sdc
    in = Resources.getResource("sample_sdc.sdc").openStream();
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "sample_sdc.sdc", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    //xml
    in = Resources.getResource("sample_xml.xml").openStream();
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "sample_xml.xml", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    //json
    in = Resources.getResource("sample_json.json").openStream();
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "sample_json.json", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    //log
    in = Resources.getResource("sample_log.log").openStream();
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "sample_log.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    //avro
    in = Resources.getResource("sample_avro.avro").openStream();
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "sample_avro.avro", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);


    int count = 0;
    if(s3client.doesBucketExist(BUCKET_NAME)) {
      for(S3ObjectSummary s : S3Objects.withPrefix(s3client, BUCKET_NAME, "")) {
        System.out.println(s.getKey());
        count++;
      }
    }
    Assert.assertEquals(6, count);
  }

  @Test
  public void testProduceLogFile() throws Exception {
    AmazonS3Source source = createSourceLog();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      int initialCount = getObjectCount(s3client, BUCKET_NAME);

      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 2; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 60000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }

      Assert.assertEquals(10, allRecords.size());
      Assert.assertTrue(offset.contains("sample_log.log::-1"));
      Assert.assertEquals(initialCount, getObjectCount(s3client, BUCKET_NAME));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceDelimitedFile() throws Exception {
    AmazonS3Source source = createSourceDelimited();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      int initialCount = getObjectCount(s3client, BUCKET_NAME);

      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 2; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 60000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }

      Assert.assertEquals(12, allRecords.size());
      Assert.assertTrue(offset.contains("sample_csv.csv::-1"));
      Assert.assertEquals(initialCount, getObjectCount(s3client, BUCKET_NAME));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceSdcFile() throws Exception {
    AmazonS3Source source = createSourceSdc();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      int initialCount = getObjectCount(s3client, BUCKET_NAME);

      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 2; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 60000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }

      Assert.assertEquals(1023, allRecords.size());
      Assert.assertTrue(offset.contains("sample_sdc.sdc::-1"));
      Assert.assertEquals(initialCount, getObjectCount(s3client, BUCKET_NAME));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceXmlFile() throws Exception {
    AmazonS3Source source = createSourceXml();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      int initialCount = getObjectCount(s3client, BUCKET_NAME);

      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 2; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 60000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }

      Assert.assertEquals(12, allRecords.size());
      Assert.assertTrue(offset.contains("sample_xml.xml::-1"));
      Assert.assertEquals(initialCount, getObjectCount(s3client, BUCKET_NAME));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceJsonFile() throws Exception {
    AmazonS3Source source = createSourceJson();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      int initialCount = getObjectCount(s3client, BUCKET_NAME);

      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 2; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 60000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }

      Assert.assertEquals(3, allRecords.size());
      Assert.assertTrue(offset.contains("sample_json.json::-1"));
      Assert.assertEquals(initialCount, getObjectCount(s3client, BUCKET_NAME));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceAvroFile() throws Exception {
    AmazonS3Source source = createSourceAvro();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      int initialCount = getObjectCount(s3client, BUCKET_NAME);

      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 5; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 1000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }

      Assert.assertEquals(3, allRecords.size());
      Assert.assertTrue(offset.contains("sample_avro.avro::-1"));
      Assert.assertEquals(initialCount, getObjectCount(s3client, BUCKET_NAME));

    } finally {
      runner.runDestroy();
    }
  }

  private AmazonS3Source createSourceLog() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataParserFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.LOG;
    s3ConfigBean.dataFormatConfig.logMode = LogMode.COMMON_LOG_FORMAT;
    s3ConfigBean.dataFormatConfig.logMaxObjectLen = 1024;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.log";

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.setEndPointForTest("http://localhost:" + port);
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.advancedConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceDelimited() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataParserFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.DELIMITED;
    s3ConfigBean.dataFormatConfig.csvFileFormat = CsvMode.CSV;
    s3ConfigBean.dataFormatConfig.csvMaxObjectLen = 1024;
    s3ConfigBean.dataFormatConfig.csvHeader = CsvHeader.IGNORE_HEADER;
    s3ConfigBean.dataFormatConfig.csvRecordType = CsvRecordType.LIST;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.csv";

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.setEndPointForTest("http://localhost:" + port);
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.advancedConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceSdc() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataParserFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.SDC_JSON;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.sdc";

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.setEndPointForTest("http://localhost:" + port);
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.advancedConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceXml() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataParserFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.XML;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";
    s3ConfigBean.dataFormatConfig.xmlMaxObjectLen = 1024;
    s3ConfigBean.dataFormatConfig.xmlRecordElement = "book";

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.xml";

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.setEndPointForTest("http://localhost:" + port);
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.advancedConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceJson() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataParserFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.JSON;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";
    s3ConfigBean.dataFormatConfig.jsonMaxObjectLen = 10000;
    s3ConfigBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.json";

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.setEndPointForTest("http://localhost:" + port);
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.advancedConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createSourceAvro() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataParserFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.AVRO;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";
    s3ConfigBean.dataFormatConfig.avroSchema = null;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 128;
    s3ConfigBean.s3FileConfig.prefixPattern = "*.avro";

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.setEndPointForTest("http://localhost:" + port);
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.advancedConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private int getObjectCount(AmazonS3Client s3Client, String bucket) {
    int count = 0;
    for(S3ObjectSummary s : S3Objects.inBucket(s3Client, bucket)) {
      count++;
    }
    return count;
  }
}
