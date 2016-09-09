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
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.FakeS3;
import com.streamsets.pipeline.stage.common.TestUtil;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestAmazonS3Source2 {
  private static String fakeS3Root;
  private static ExecutorService executorService;
  private static FakeS3 fakeS3;
  private static AmazonS3Client s3client;
  private static final String BUCKET_NAME = "mybucket2";
  private static final String POSTPROCESS_BUCKET = "post-process-bucket2";
  private static final String POSTPROCESS_PREFIX = "post-process-prefix";
  private static final String ERROR_BUCKET = "error-bucket2";
  private static final String ERROR_PREFIX = "error-prefix";
  private static int port;

  @BeforeClass
  public static void setUpClass() throws IOException, InterruptedException, URISyntaxException {
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

  private static void populateFakes3() throws IOException, InterruptedException, URISyntaxException {
    BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");
    s3client = new AmazonS3Client(credentials);
    s3client.setEndpoint("http://localhost:" + port);
    s3client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));

    TestUtil.createBucket(s3client, BUCKET_NAME);
    TestUtil.createBucket(s3client, POSTPROCESS_BUCKET);
    TestUtil.createBucket(s3client, ERROR_BUCKET);

    //create directory structure
    // mybucket/NorthAmerica/USA
    // mybucket/NorthAmerica/Canada
    //
    //write 3 files each under myBucket, myBucket/NorthAmerica, mybucket/NorthAmerica/USA, mybucket/NorthAmerica/Canada
    //12 files in total

    InputStream in = new ByteArrayInputStream("Hello World".getBytes());
    PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, "file1.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "file2.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "file3.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/file4.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/file5.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/file6.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/USA/file7.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/USA/file8.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/USA/file9.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/Canada/file10.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/Canada/file11.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    in = new ByteArrayInputStream("Hello World".getBytes());
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/Canada/file12.log", in, new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/logArchive1.zip",
      new FileInputStream(new File(Resources.getResource("logArchive.zip").toURI())), new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/logArchive2.zip",
      new FileInputStream(new File(Resources.getResource("logArchive.zip").toURI())), new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/logArchive1.tar.gz",
      new FileInputStream(new File(Resources.getResource("logArchive.tar.gz").toURI())), new ObjectMetadata());
    s3client.putObject(putObjectRequest);
    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/logArchive2.tar.gz",
      new FileInputStream(new File(Resources.getResource("logArchive.tar.gz").toURI())), new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/testAvro1.tar.gz",
      new FileInputStream(new File(Resources.getResource("testAvro.tar.gz").toURI())), new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    putObjectRequest = new PutObjectRequest(BUCKET_NAME, "NorthAmerica/testAvro2.tar.gz",
      new FileInputStream(new File(Resources.getResource("testAvro.tar.gz").toURI())), new ObjectMetadata());
    s3client.putObject(putObjectRequest);

    int count = 0;
    if(s3client.doesBucketExist(BUCKET_NAME)) {
      for(S3ObjectSummary s : S3Objects.withPrefix(s3client, BUCKET_NAME, "")) {
        System.out.println(s.getKey());
        count++;
      }
    }
    Assert.assertEquals(18, count); //12 files + 3 dirs
  }

  @Test
  public void testProduceFullFile() throws Exception {
    AmazonS3Source source = createSource();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      int initialCount = getObjectCount(s3client, BUCKET_NAME);
      int postProcessInitialCount = getObjectCount(s3client, POSTPROCESS_BUCKET);

      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 10; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 60000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }

      Assert.assertEquals(6, allRecords.size());
      Assert.assertEquals(initialCount-6, getObjectCount(s3client, BUCKET_NAME));
      Assert.assertEquals(postProcessInitialCount + 6, getObjectCount(s3client, POSTPROCESS_BUCKET));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceZipFile() throws Exception {
    AmazonS3Source source = createZipSource();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 50; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 1000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }
      Assert.assertEquals(37044, allRecords.size());
      Assert.assertTrue(offset.contains("NorthAmerica/logArchive2.zip::-1::53a43eff19003fa36a23589c90b31bc7::"));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceTarGzipTextFile() throws Exception {
    AmazonS3Source source = createTarGzipSource();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 50; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 1000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }
      Assert.assertEquals(37044, allRecords.size());
      Assert.assertTrue(offset.contains("NorthAmerica/logArchive2.tar.gz::-1::9c91073f2c2b51ed80c0a33da1238214::"));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceTarGzipAvroFile() throws Exception {
    AmazonS3Source source = createTarGzipAvroSource();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int i = 0; i < 50; i++) {
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 1000, batchMaker);
        Assert.assertNotNull(offset);

        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        allRecords.addAll(records);
      }
      Assert.assertEquals(48000, allRecords.size());
      Assert.assertTrue(offset.contains("NorthAmerica/testAvro2.tar.gz::-1::c17d97fdd6f2c6902efe059753cf41b6::"));

    } finally {
      runner.runDestroy();
    }
  }

  private AmazonS3Source createSource() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataParserFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.TEXT;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";
    s3ConfigBean.dataFormatConfig.textMaxLineLen = 1024;
    s3ConfigBean.dataFormatConfig.compression = Compression.NONE;
    s3ConfigBean.dataFormatConfig.filePatternInArchive = "*";

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.errorConfig.archivingOption = S3ArchivingOption.MOVE_TO_PREFIX;
    s3ConfigBean.errorConfig.errorPrefix = ERROR_PREFIX;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.postProcessingConfig.postProcessBucket = POSTPROCESS_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessPrefix = POSTPROCESS_PREFIX;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*/*/*.log";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createZipSource() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataParserFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.TEXT;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";
    s3ConfigBean.dataFormatConfig.textMaxLineLen = 102400;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*/*.zip";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.dataFormatConfig.compression = Compression.ARCHIVE;
    s3ConfigBean.dataFormatConfig.filePatternInArchive = "*/*.log";

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createTarGzipSource() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataParserFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.TEXT;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";
    s3ConfigBean.dataFormatConfig.textMaxLineLen = 102400;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*/logArchive*.tar.gz";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.dataFormatConfig.compression = Compression.COMPRESSED_ARCHIVE;
    s3ConfigBean.dataFormatConfig.filePatternInArchive = "*/[!.]*.log";

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
    return new AmazonS3Source(s3ConfigBean);
  }

  private AmazonS3Source createTarGzipAvroSource() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataParserFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.AVRO;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";
    s3ConfigBean.dataFormatConfig.textMaxLineLen = 102400;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.NONE;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.prefixPattern = "*/testAvro*.tar.gz";
    s3ConfigBean.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    s3ConfigBean.dataFormatConfig.compression = Compression.COMPRESSED_ARCHIVE;
    s3ConfigBean.dataFormatConfig.filePatternInArchive = "[!.]*.avro";

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.region = AWSRegions.OTHER;
    s3ConfigBean.s3Config.endpoint = "http://localhost:" + port;
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.awsConfig = new AWSConfig();
    s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = "foo";
    s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3ConfigBean.s3Config.commonPrefix = "";
    s3ConfigBean.s3Config.delimiter = "/";
    s3ConfigBean.proxyConfig = new ProxyConfig();
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
