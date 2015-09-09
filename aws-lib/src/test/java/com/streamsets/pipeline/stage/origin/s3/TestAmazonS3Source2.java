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
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataFormatConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
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
  private static final String POSTPROCESS_FOLDER = "post-process-folder";
  private static final String ERROR_BUCKET = "error-bucket2";
  private static final String ERROR_FOLDER = "error-folder";
  private static int port;

  @BeforeClass
  public static void setUpClass() throws IOException, InterruptedException {
    File dir = new File(new File("target", UUID.randomUUID().toString()), "fakes3_root").getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    fakeS3Root = dir.getAbsolutePath();
    port = getFreePort();
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

    createBucket(s3client, BUCKET_NAME);
    createBucket(s3client, POSTPROCESS_BUCKET);
    createBucket(s3client, ERROR_BUCKET);

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

    int count = 0;
    if(s3client.doesBucketExist(BUCKET_NAME)) {
      for(S3ObjectSummary s : S3Objects.withPrefix(s3client, BUCKET_NAME, "")) {
        System.out.println(s.getKey());
        count++;
      }
    }
    Assert.assertEquals(12, count); //12 files + 3 dirs
  }

  private static void createBucket(AmazonS3Client s3client, String bucketName) {
    if(s3client.doesBucketExist(bucketName)) {
      for(S3ObjectSummary s : S3Objects.inBucket(s3client, bucketName)) {
        s3client.deleteObject(bucketName, s.getKey());
      }
      s3client.deleteBucket(bucketName);
    }
    Assert.assertFalse(s3client.doesBucketExist(bucketName));
    // Note that CreateBucketRequest does not specify region. So bucket is
    // bucketName
    s3client.createBucket(new CreateBucketRequest(bucketName));
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
      Assert.assertEquals(postProcessInitialCount+6, getObjectCount(s3client, POSTPROCESS_BUCKET));

    } finally {
      runner.runDestroy();
    }
  }

  private AmazonS3Source createSource() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataFormatConfig();
    s3ConfigBean.dataFormat = DataFormat.TEXT;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";
    s3ConfigBean.dataFormatConfig.textMaxLineLen = 1024;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorHandlingOption = PostProcessingOptions.NONE;
    s3ConfigBean.errorConfig.errorFolder = ERROR_FOLDER;
    s3ConfigBean.errorConfig.errorBucket = ERROR_BUCKET;

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.postProcessingConfig.postProcessBucket = POSTPROCESS_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessFolder = POSTPROCESS_FOLDER;

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.overrunLimit = 65;
    s3ConfigBean.s3FileConfig.filePattern = "*/*/*.log";

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.setEndPointForTest("http://localhost:" + port);
    s3ConfigBean.s3Config.bucket = BUCKET_NAME;
    s3ConfigBean.s3Config.accessKeyId = "foo";
    s3ConfigBean.s3Config.secretAccessKey = "bar";
    s3ConfigBean.s3Config.folder = "";
    s3ConfigBean.s3Config.delimiter = "/";

    return new AmazonS3Source(s3ConfigBean);
  }

  public static int getFreePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    return port;
  }

  private int getObjectCount(AmazonS3Client s3Client, String bucket) {
    int count = 0;
    for(S3ObjectSummary s : S3Objects.inBucket(s3Client, bucket)) {
      count++;
    }
    return count;
  }
}
