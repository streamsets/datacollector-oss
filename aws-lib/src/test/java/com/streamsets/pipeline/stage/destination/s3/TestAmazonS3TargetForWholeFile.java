/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.ChecksumAlgorithm;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.io.fileref.LocalFileRef;
import com.streamsets.pipeline.sdk.RecordCreator;
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
import com.streamsets.pipeline.stage.origin.s3.S3FileRef;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class TestAmazonS3TargetForWholeFile {
  private static final String SOURCE_BUCKET_NAME = "source";
  private static final String TARGET_BUCKET_NAME = "target";
  private static final String FILE_NAME_1 = "file1.txt";
  private static final String FILE_NAME_2= "file2.txt";
  private static final String OBJECT_KEY_1 = "file1.txt";
  private static final String OBJECT_KEY_2 = "file2.txt";
  private static final String DELIMITER = "/";
  private static final String SAMPLE_TEXT_TO_FILE_PATH_1 = "Sample text to file path 1";
  private static final String SAMPLE_TEXT_TO_FILE_PATH_2 = "Sample text to file path 2";
  private static final Map<String, String> SAMPLE_TEXT_FOR_FILE =
      ImmutableMap.of(FILE_NAME_1, SAMPLE_TEXT_TO_FILE_PATH_1, FILE_NAME_2, SAMPLE_TEXT_TO_FILE_PATH_2);


  private static String fakeS3Root;
  private static FakeS3 fakeS3;
  private static AmazonS3Client s3client;
  private static ExecutorService executorService;
  private static int port;
  private static File testDir;

  private enum SourceType {
    LOCAL,
    S3
  }

  private final String fileNameEL;
  private final SourceType source;
  private final ChecksumAlgorithm checksumAlgorithm;
  private final boolean withFileNamePrefix;


  public TestAmazonS3TargetForWholeFile(
      String fileNameEL,
      boolean withFileNamePrefix,
      SourceType source,
      ChecksumAlgorithm checksumAlgorithm
      ) {
    this.fileNameEL = fileNameEL;
    this.withFileNamePrefix = withFileNamePrefix;
    this.source = source;
    this.checksumAlgorithm = checksumAlgorithm;
  }

  @Parameterized.Parameters(name = "File Name Prefix : {1}, Source Type: {2}, , Checksum Algorithm: {2}")
  public static Collection<Object[]> data() throws Exception {
    List<Object[]> finalData = new ArrayList<>();
    List<Object[]> array = Arrays.asList(new Object[][]{
        {"${record:value('/fileInfo/filename')}", true, SourceType.LOCAL},
        {"${record:value('/fileInfo/filename')}", false, SourceType.LOCAL},
        {"${record:value('/fileInfo/objectKey')}", true, SourceType.S3},
        {"${record:value('/fileInfo/objectKey')}", false, SourceType.S3}
    });
    List<ChecksumAlgorithm> supportedChecksumAlgorithms =
        Arrays.asList(
            ChecksumAlgorithm.values()
        );
    for (ChecksumAlgorithm checksumAlgorithm : supportedChecksumAlgorithms) {
      for (int j = 0; j < array.size(); j++) {
        Object[] data = new Object[4];
        data[0] = array.get(j)[0];
        data[1] = array.get(j)[1];
        data[2] = array.get(j)[2];
        data[3] = checksumAlgorithm;
        finalData.add(data);
      }
    }
    return finalData;
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
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

    TestUtil.createBucket(s3client, SOURCE_BUCKET_NAME);
    TestUtil.createBucket(s3client, TARGET_BUCKET_NAME);

    testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());

    String filePath1 = testDir.getAbsolutePath() + "/" + FILE_NAME_1;
    String filePath2 = testDir.getAbsolutePath() + "/" + FILE_NAME_2;

    writeTextToFile(filePath1, SAMPLE_TEXT_TO_FILE_PATH_1);
    writeTextToFile(filePath2, SAMPLE_TEXT_TO_FILE_PATH_2);

    s3client.putObject(new PutObjectRequest(SOURCE_BUCKET_NAME, OBJECT_KEY_1, new File(filePath1)));
    s3client.putObject(new PutObjectRequest(SOURCE_BUCKET_NAME, OBJECT_KEY_2, new File(filePath2)));
  }

  private static void writeTextToFile(String filePath, String text) throws Exception {
    Files.write(
        Paths.get(filePath),
        text.getBytes(),
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE_NEW
    );
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

  public void deleteObjectsAfterVerificationInTarget(String objectKey) throws Exception {
    s3client.deleteObject(new DeleteObjectRequest(TARGET_BUCKET_NAME, objectKey));
  }


  public static Map<String, Object> getLocalFileMetadata(String filePath) throws Exception {
    String attributesToRead =
        Paths.get(filePath).getFileSystem().supportedFileAttributeViews().contains("posix")? "posix:*" : "*";
    Map<String, Object> metadata = new HashMap<>(Files.readAttributes(Paths.get(filePath), attributesToRead));
    metadata.put("filename", Paths.get(filePath).getFileName().toString());
    metadata.put("file", filePath);
    return metadata;
  }

  private static List<Record> createRecordsForWholeFileFromLocalFS() throws Exception {
    List<Record> records = new ArrayList<>();
    for (String fileName : Arrays.asList(FILE_NAME_1, FILE_NAME_2)) {
      Record record = RecordCreator.create();
      record.set(
          FileRefUtil.getWholeFileRecordRootField(
              new LocalFileRef.Builder()
                  .filePath(testDir.getAbsolutePath() + "/" + fileName)
                  .bufferSize(1024)
                  .build(),
              getLocalFileMetadata(testDir.getAbsolutePath() + "/" + fileName)
          )
      );
      records.add(record);
    }
    return records;
  }

  private static Map<String, Object> getS3Metadata(S3Object s3Object) {
    Map<String, Object> metaDataMap = new HashMap<>();
    metaDataMap.put(Headers.CACHE_CONTROL, s3Object.getObjectMetadata().getCacheControl());
    metaDataMap.put(Headers.CONTENT_DISPOSITION, s3Object.getObjectMetadata().getContentDisposition());
    metaDataMap.put(Headers.CONTENT_ENCODING, s3Object.getObjectMetadata().getContentEncoding());
    metaDataMap.put(Headers.CONTENT_LENGTH, s3Object.getObjectMetadata().getContentLength());
    metaDataMap.put(Headers.CONTENT_RANGE, s3Object.getObjectMetadata().getInstanceLength());
    metaDataMap.put(Headers.CONTENT_MD5, s3Object.getObjectMetadata().getContentMD5());
    metaDataMap.put(Headers.CONTENT_TYPE, s3Object.getObjectMetadata().getContentType());
    metaDataMap.put(Headers.EXPIRES, s3Object.getObjectMetadata().getExpirationTime());
    metaDataMap.put(Headers.ETAG, s3Object.getObjectMetadata().getETag());
    metaDataMap.put(Headers.LAST_MODIFIED, s3Object.getObjectMetadata().getLastModified());
    metaDataMap.put("bucket", s3Object.getBucketName());
    metaDataMap.put("objectKey", s3Object.getKey());
    metaDataMap.put("size", s3Object.getObjectMetadata().getContentLength());
    return metaDataMap;
  }

  private static List<Record> createRecordsForWholeFileFromS3() throws Exception {
    Iterator<S3ObjectSummary> s3ObjectSummaryIterator = S3Objects.inBucket(s3client, SOURCE_BUCKET_NAME).iterator();
    List<Record> records = new ArrayList<>();
    while (s3ObjectSummaryIterator.hasNext()) {
      S3ObjectSummary s3ObjectSummary = s3ObjectSummaryIterator.next();
      Map<String, Object> metadata = getS3Metadata(s3client.getObject(SOURCE_BUCKET_NAME, s3ObjectSummary.getKey()));
      Record record = RecordCreator.create();
      record.set(
          FileRefUtil.getWholeFileRecordRootField(
              new S3FileRef.Builder().s3Client(s3client)
                  .verifyChecksum(true)
                  .s3ObjectSummary(s3ObjectSummary)
                  .verifyChecksum(false)
                  .bufferSize(1024)
                  .build(),
              metadata
          )
      );
      records.add(record);
    }
    return records;
  }

  private List<Record> getRecords() throws Exception {
    switch (source) {
      case LOCAL:
        return createRecordsForWholeFileFromLocalFS();
      case S3:
        return createRecordsForWholeFileFromS3();
      default: throw new IllegalArgumentException("Unsupported type " + source.name());
    }
  }

  private AmazonS3Target createS3targetWithWholeFile() {
    S3Config s3Config = new S3Config();
    s3Config.region = AWSRegions.OTHER;
    s3Config.endpoint = "http://localhost:" + port;
    s3Config.bucket = TARGET_BUCKET_NAME;
    s3Config.awsConfig = new AWSConfig();
    s3Config.awsConfig.awsAccessKeyId = "foo";
    s3Config.awsConfig.awsSecretAccessKey = "bar";
    s3Config.commonPrefix = "";
    s3Config.delimiter = DELIMITER;

    S3TargetConfigBean s3TargetConfigBean = new S3TargetConfigBean();
    s3TargetConfigBean.dataFormat = DataFormat.WHOLE_FILE;
    s3TargetConfigBean.partitionTemplate = "";
    s3TargetConfigBean.fileNamePrefix = withFileNamePrefix? "sdc" : "";
    s3TargetConfigBean.timeDriverTemplate = "${time:now()}";
    s3TargetConfigBean.timeZoneID = "UTC";
    s3TargetConfigBean.s3Config = s3Config;
    s3TargetConfigBean.sseConfig = new SSEConfigBean();
    s3TargetConfigBean.proxyConfig = new ProxyConfig();
    s3TargetConfigBean.tmConfig = new TransferManagerConfig();
    s3TargetConfigBean.tmConfig.threadPoolSize = 3;

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.fileNameEL = fileNameEL;
    dataGeneratorFormatConfig.wholeFileExistsAction = WholeFileExistsAction.OVERWRITE;
    dataGeneratorFormatConfig.includeChecksumInTheEvents = true;
    dataGeneratorFormatConfig.checksumAlgorithm = checksumAlgorithm;

    s3TargetConfigBean.dataGeneratorFormatConfig = dataGeneratorFormatConfig;

    return new AmazonS3Target(s3TargetConfigBean);
  }

  private static void verifyStreamCorrectness(InputStream is1, InputStream is2) throws Exception {
    int totalBytesRead1 = 0, totalBytesRead2 = 0;
    int a = 0, b = 0;
    while (a != -1 || b != -1) {
      totalBytesRead1 = ((a = is1.read()) != -1)? totalBytesRead1 + 1 : totalBytesRead1;
      totalBytesRead2 = ((b = is2.read()) != -1)? totalBytesRead2 + 1 : totalBytesRead2;
      Assert.assertEquals(a, b);
    }
    Assert.assertEquals(totalBytesRead1, totalBytesRead2);
  }


  private int verifyAndReturnNoOfObjects() throws Exception {
    int numberOfObjects = 0;
    Iterator<S3ObjectSummary> s3ObjectSummaryIterator = S3Objects.inBucket(s3client, TARGET_BUCKET_NAME).iterator();
    while (s3ObjectSummaryIterator.hasNext()) {
      S3ObjectSummary s3ObjectSummary = s3ObjectSummaryIterator.next();
      String fileNameOrKey = s3ObjectSummary.getKey();
      if (withFileNamePrefix) {
        //strip out the filePrefix sdc-
        fileNameOrKey = fileNameOrKey.substring(4);
      }
      switch (source) {
        case LOCAL:
          verifyStreamCorrectness(
              new FileInputStream(testDir.getAbsolutePath() + "/" + fileNameOrKey),
              s3client.getObject(TARGET_BUCKET_NAME, s3ObjectSummary.getKey()).getObjectContent()
          );
          break;
        case S3:
          verifyStreamCorrectness(
              s3client.getObject(SOURCE_BUCKET_NAME, fileNameOrKey).getObjectContent(),
              s3client.getObject(TARGET_BUCKET_NAME, s3ObjectSummary.getKey()).getObjectContent()
          );
          break;
      }
      deleteObjectsAfterVerificationInTarget(s3ObjectSummary.getKey());
      numberOfObjects++;
    }
    return numberOfObjects;
  }

  @Test
  public void testWholeFileRecordWrites() throws Exception {
    AmazonS3Target amazonS3Target = createS3targetWithWholeFile();
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target).build();
    targetRunner.runInit();
    try {
      targetRunner.runWrite(getRecords());

      int numberOfRecords = verifyAndReturnNoOfObjects();

      Assert.assertEquals(numberOfRecords, targetRunner.getEventRecords().size());
      Iterator<Record> eventRecordIterator = targetRunner.getEventRecords().iterator();

      while (eventRecordIterator.hasNext()) {
        Record eventRecord = eventRecordIterator.next();
        Assert.assertTrue(eventRecord.has(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO_PATH));
        Assert.assertTrue(eventRecord.has(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO_PATH));
        Map<String, Field> targetFileInfo = eventRecord.get(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO_PATH).getValueAsMap();
        Assert.assertEquals(targetFileInfo.get("bucket").getValueAsString(), TARGET_BUCKET_NAME);

        Assert.assertTrue(eventRecord.has("/" + FileRefUtil.WHOLE_FILE_CHECKSUM_ALGO));
        Assert.assertTrue(eventRecord.has("/" + FileRefUtil.WHOLE_FILE_CHECKSUM));

        Assert.assertEquals(
            checksumAlgorithm.name(),
            eventRecord.get("/" + FileRefUtil.WHOLE_FILE_CHECKSUM_ALGO).getValueAsString()
        );

        //strip out the filePrefix sdc-
        String objectKey =
            eventRecord.get(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO_PATH + "/objectKey").getValueAsString();
        if (withFileNamePrefix) {
          Assert.assertTrue(objectKey.startsWith("sdc-"));
          //strip out the filePrefix sdc-
          objectKey = objectKey.substring(4);
        }

        String checksum = HashingUtil.getHasher(checksumAlgorithm.getHashType())
            .hashString(SAMPLE_TEXT_FOR_FILE.get(objectKey), Charset.defaultCharset()).toString();
        Assert.assertEquals(checksum, eventRecord.get("/" + FileRefUtil.WHOLE_FILE_CHECKSUM).getValueAsString());
      }
    } finally {
      targetRunner.runDestroy();
    }
  }

}
