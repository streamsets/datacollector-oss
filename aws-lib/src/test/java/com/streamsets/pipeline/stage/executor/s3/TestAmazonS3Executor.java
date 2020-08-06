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
package com.streamsets.pipeline.stage.executor.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.Tag;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.common.FakeS3;
import com.streamsets.pipeline.stage.common.TestUtil;
import com.streamsets.pipeline.stage.executor.s3.config.AmazonS3ExecutorConfig;
import com.streamsets.pipeline.stage.executor.s3.config.TaskType;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestAmazonS3Executor {

  private static final String BUCKET_NAME = "mybucket";
  private static final String SECOND_BUCKET_NAME = "yourbucket";

  private static String fakeS3Root;
  private static FakeS3 fakeS3;
  private static AmazonS3 s3client;
  private static ExecutorService executorService;
  private static int port;

  private String objectName;

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

  @Before
  public void generateObjectName() {
    objectName = UUID.randomUUID().toString();
  }

  @Test
  public void testCreateObject() throws Exception {
    AmazonS3ExecutorConfig config = getConfig();
    config.taskConfig.taskType = TaskType.CREATE_NEW_OBJECT;
    config.taskConfig.content = "${record:value('/content')}";

    AmazonS3Executor executor = new AmazonS3Executor(config);
    TargetRunner runner = new TargetRunner.Builder(AmazonS3DExecutor.class, executor)
      .build();
    runner.runInit();

    try {
      runner.runWrite(ImmutableList.of(getTestRecord()));

      //Make sure the prefix is empty
      ObjectListing objectListing = s3client.listObjects(BUCKET_NAME, objectName);
      Assert.assertEquals(1, objectListing.getObjectSummaries().size());

      S3Object object = s3client.getObject(BUCKET_NAME, objectName);
      S3ObjectInputStream objectContent = object.getObjectContent();

      List<String> stringList = IOUtils.readLines(objectContent);
      Assert.assertEquals(1, stringList.size());
      Assert.assertEquals("Secret", stringList.get(0));

      Assert.assertEquals(1, runner.getEventRecords().size());
      assertEvent(runner.getEventRecords().get(0), objectName);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCopyObject() throws Exception {
    String newName = UUID.randomUUID().toString();

    AmazonS3ExecutorConfig config = getConfig();
    config.taskConfig.taskType = TaskType.COPY_OBJECT;
    config.taskConfig.copyTargetLocation = newName;

    AmazonS3Executor executor = new AmazonS3Executor(config);
    TargetRunner runner = new TargetRunner.Builder(AmazonS3DExecutor.class, executor)
      .build();
    runner.runInit();

    try {
      s3client.putObject(new PutObjectRequest(BUCKET_NAME, objectName, IOUtils.toInputStream("content"), new ObjectMetadata()));
      runner.runWrite(ImmutableList.of(getTestRecord()));

      S3Object object = s3client.getObject(BUCKET_NAME, newName);
      S3ObjectInputStream objectContent = object.getObjectContent();

      List<String> stringList = IOUtils.readLines(objectContent);
      Assert.assertEquals(1, stringList.size());
      Assert.assertEquals("content", stringList.get(0));

      Assert.assertTrue(s3client.doesObjectExist(BUCKET_NAME, objectName));

      Assert.assertEquals(1, runner.getEventRecords().size());
      assertEvent(runner.getEventRecords().get(0), newName);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCopyObjectDeleteOriginal() throws Exception {
    String newName = UUID.randomUUID().toString();

    AmazonS3ExecutorConfig config = getConfig();
    config.taskConfig.taskType = TaskType.COPY_OBJECT;
    config.taskConfig.dropAfterCopy = true;
    config.taskConfig.copyTargetLocation = newName;

    AmazonS3Executor executor = new AmazonS3Executor(config);
    TargetRunner runner = new TargetRunner.Builder(AmazonS3DExecutor.class, executor)
      .build();
    runner.runInit();

    try {
      s3client.putObject(new PutObjectRequest(BUCKET_NAME, objectName, IOUtils.toInputStream("dropAfterCopy"), new ObjectMetadata()));
      runner.runWrite(ImmutableList.of(getTestRecord()));

      S3Object object = s3client.getObject(BUCKET_NAME, newName);
      S3ObjectInputStream objectContent = object.getObjectContent();

      List<String> stringList = IOUtils.readLines(objectContent);
      Assert.assertEquals(1, stringList.size());
      Assert.assertEquals("dropAfterCopy", stringList.get(0));

      Assert.assertFalse(s3client.doesObjectExist(BUCKET_NAME, objectName));

      Assert.assertEquals(1, runner.getEventRecords().size());
      assertEvent(runner.getEventRecords().get(0), newName);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testApplyTags() throws Exception {
    AmazonS3ExecutorConfig config = getConfig();
    config.taskConfig.tags = ImmutableMap.of(
      "${record:value('/key')}", "${record:value('/value')}"
    );

    AmazonS3Executor executor = new AmazonS3Executor(config);
    TargetRunner runner = new TargetRunner.Builder(AmazonS3DExecutor.class, executor)
      .build();
    runner.runInit();

    try {
      s3client.putObject(new PutObjectRequest(BUCKET_NAME, objectName, IOUtils.toInputStream("content"), new ObjectMetadata()));
      runner.runWrite(ImmutableList.of(getTestRecord()));

      List<Tag> tags = s3client.getObjectTagging(new GetObjectTaggingRequest(BUCKET_NAME, objectName)).getTagSet();
      Assert.assertEquals(1, tags.size());
      Assert.assertEquals("Owner", tags.get(0).getKey());
      Assert.assertEquals("Earth", tags.get(0).getValue());

      Assert.assertEquals(1, runner.getEventRecords().size());
      assertEvent(runner.getEventRecords().get(0), objectName);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testApplyTagsBrokenEL() throws Exception {
    AmazonS3ExecutorConfig config = getConfig();
    config.taskConfig.tags = ImmutableMap.of(
      "${record:value('/ke", "${record:value('/value')}"
    );

    AmazonS3Executor executor = new AmazonS3Executor(config);
    TargetRunner runner = new TargetRunner.Builder(AmazonS3DExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    try {
      runner.runWrite(ImmutableList.of(getTestRecord()));

      Assert.assertEquals(1, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEmptyBucket() throws Exception {
    AmazonS3ExecutorConfig config = getConfig();
    config.s3Config.bucketTemplate = "";

    AmazonS3Executor executor = new AmazonS3Executor(config);
    TargetRunner runner = new TargetRunner.Builder(AmazonS3DExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    try {
      runner.runWrite(ImmutableList.of(getTestRecord()));

      Assert.assertEquals(1, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEmptyObjectPath() throws Exception {
    AmazonS3ExecutorConfig config = getConfig();
    config.taskConfig.objectPath = "";

    AmazonS3Executor executor = new AmazonS3Executor(config);
    TargetRunner runner = new TargetRunner.Builder(AmazonS3DExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    try {
      runner.runWrite(ImmutableList.of(getTestRecord()));

      Assert.assertEquals(1, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  private AmazonS3ExecutorConfig getConfig() {
    AmazonS3ExecutorConfig config = new AmazonS3ExecutorConfig();

    config.s3Config.connection.useRegion = true;
    config.s3Config.connection.region = AwsRegion.OTHER;
    config.s3Config.connection.endpoint = "http://localhost:" + port;
    config.s3Config.bucketTemplate = "${record:attribute('bucket')}";
    config.s3Config.connection.awsConfig = new AWSConfig();
    config.s3Config.connection.awsConfig.awsAccessKeyId = () -> "foo";
    config.s3Config.connection.awsConfig.awsSecretAccessKey = () -> "bar";
    config.s3Config.connection.awsConfig.disableChunkedEncoding = true;

    config.taskConfig.taskType = TaskType.CHANGE_EXISTING_OBJECT;
    config.taskConfig.objectPath = "${record:value('/object')}";

    return config;
  }

  private Record getTestRecord() {
    Record record = RecordCreator.create();
    record.getHeader().setAttribute("bucket", BUCKET_NAME);
    record.set(Field.create(Field.Type.MAP, ImmutableMap.of(
      "object", Field.create(objectName),
      "key", Field.create("Owner"),
      "value", Field.create("Earth"),
      "content", Field.create("Secret")
    )));

    return record;
  }

  private void assertEvent(EventRecord eventRecord, String objectName) {
    Assert.assertNotNull(eventRecord);
    Assert.assertNotNull(objectName);

    Assert.assertTrue(eventRecord.has("/object_key"));
    Assert.assertEquals(objectName, eventRecord.get("/object_key").getValueAsString());
  }

}
