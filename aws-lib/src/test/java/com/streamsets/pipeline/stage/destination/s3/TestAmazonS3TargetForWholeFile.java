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
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.WholeFileChecksumAlgorithm;
import com.streamsets.pipeline.config.ChecksumAlgorithm;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.io.fileref.LocalFileRef;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.sdk.service.SdkWholeFileDataFormatGeneratorService;
import com.streamsets.pipeline.stage.common.AmazonS3TestSuite;
import com.streamsets.pipeline.stage.common.TestUtil;
import com.streamsets.pipeline.stage.common.s3.AwsS3Connection;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.lib.aws.TransferManagerConfig;
import com.streamsets.pipeline.stage.origin.s3.S3FileRef;
import org.junit.AfterClass;
import org.junit.Assert;
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

@RunWith(Parameterized.class)
public class TestAmazonS3TargetForWholeFile extends AmazonS3TestSuite {
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

  private static AmazonS3 s3client;
  private static File testDir;

  private enum SourceType {
    LOCAL,
    S3
  }

  private final String fileNamePath;
  private final SourceType source;
  private final WholeFileChecksumAlgorithm checksumAlgorithm;
  private final boolean withFileNamePrefix;


  public TestAmazonS3TargetForWholeFile(
      String fileNamePath,
      boolean withFileNamePrefix,
      SourceType source,
      WholeFileChecksumAlgorithm checksumAlgorithm
  ) {
    this.fileNamePath = fileNamePath;
    this.withFileNamePrefix = withFileNamePrefix;
    this.source = source;
    this.checksumAlgorithm = checksumAlgorithm;
  }

  @Parameterized.Parameters(name = "File Name Prefix : {1}, Source Type: {2}, , Checksum Algorithm: {3}")
  public static Collection<Object[]> data() throws Exception {
    List<Object[]> finalData = new ArrayList<>();
    List<Object[]> array = Arrays.asList(new Object[][]{
        {"/fileInfo/filename", true, SourceType.LOCAL},
        {"/fileInfo/filename", false, SourceType.LOCAL},
        {"/fileInfo/objectKey", true, SourceType.S3},
        {"/fileInfo/objectKey", false, SourceType.S3}
    });
    List<WholeFileChecksumAlgorithm> supportedChecksumAlgorithms =
        Arrays.asList(
            WholeFileChecksumAlgorithm.values()
        );
    for (WholeFileChecksumAlgorithm checksumAlgorithm : supportedChecksumAlgorithms) {
      for (Object[] anArray : array) {
        Object[] data = new Object[4];
        data[0] = anArray[0];
        data[1] = anArray[1];
        data[2] = anArray[2];
        data[3] = checksumAlgorithm;
        finalData.add(data);
      }
    }
    return finalData;
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    setupS3();

    BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");
    s3client = AmazonS3ClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:" + port, null))
        .withPathStyleAccessEnabled(true)
        .withChunkedEncodingDisabled(true)
        .build();

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
    teardownS3();
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
      record.getHeader().setAttribute("bucket", TARGET_BUCKET_NAME);
      record.set(
          FileRefUtil.getWholeFileRecordRootField(
              new LocalFileRef.Builder()
                  .filePath(testDir.getAbsolutePath() + "/" + fileName)
                  .bufferSize(1024)
                  .verifyChecksum(false)
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
      record.getHeader().setAttribute("bucket", TARGET_BUCKET_NAME);
      record.set(
          FileRefUtil.getWholeFileRecordRootField(
              new S3FileRef.Builder()
                  .s3Client(s3client)
                  .s3ObjectSummary(s3ObjectSummary)
                  .useSSE(false)
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
    s3Config.commonPrefix = "";
    s3Config.delimiter = DELIMITER;
    s3Config.connection.proxyConfig = new ProxyConfig();

    S3TargetConfigBean s3TargetConfigBean = new S3TargetConfigBean();
    s3TargetConfigBean.partitionTemplate = "";
    s3TargetConfigBean.fileNamePrefix = withFileNamePrefix? "sdc" : "";
    s3TargetConfigBean.timeDriverTemplate = "${time:now()}";
    s3TargetConfigBean.timeZoneID = "UTC";
    s3TargetConfigBean.s3Config = s3Config;
    s3TargetConfigBean.sseConfig = new S3TargetSSEConfigBean();
    s3TargetConfigBean.tmConfig = new TransferManagerConfig();
    s3TargetConfigBean.tmConfig.threadPoolSize = 3;

    return new AmazonS3Target(s3TargetConfigBean, false);
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
    for (S3ObjectSummary s3ObjectSummary : S3Objects.inBucket(s3client, TARGET_BUCKET_NAME)) {
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
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target)
      .addService(DataFormatGeneratorService.class, new SdkWholeFileDataFormatGeneratorService(
        fileNamePath,
        com.streamsets.pipeline.api.service.dataformats.WholeFileExistsAction.OVERWRITE,
        true,
        checksumAlgorithm
        ))
      .build();
    targetRunner.runInit();
    try {
      targetRunner.runWrite(getRecords());

      int numberOfRecords = verifyAndReturnNoOfObjects();

      Assert.assertEquals(numberOfRecords, targetRunner.getEventRecords().size());

      for (Record eventRecord : targetRunner.getEventRecords()) {
        Assert.assertTrue(eventRecord.has(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO_PATH));
        Assert.assertTrue(eventRecord.has(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO_PATH));
        Map<String, Field> targetFileInfo = eventRecord.get(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO_PATH)
            .getValueAsMap();
        Assert.assertEquals(targetFileInfo.get("bucket").getValueAsString(), TARGET_BUCKET_NAME);

        Assert.assertTrue(eventRecord.has("/" + WholeFileProcessedEvent.CHECKSUM_ALGORITHM));
        Assert.assertTrue(eventRecord.has("/" + WholeFileProcessedEvent.CHECKSUM));

        Assert.assertEquals(checksumAlgorithm.name(),
            eventRecord.get("/" + WholeFileProcessedEvent.CHECKSUM_ALGORITHM).getValueAsString()
        );

        //strip out the filePrefix sdc-
        String objectKey = eventRecord.get(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO_PATH + "/objectKey")
            .getValueAsString();
        if (withFileNamePrefix) {
          Assert.assertTrue(objectKey.startsWith("sdc-"));
          //strip out the filePrefix sdc-
          objectKey = objectKey.substring(4);
        }

        String checksum = HashingUtil.getHasher(ChecksumAlgorithm.forApi(checksumAlgorithm).getHashType()).hashString(SAMPLE_TEXT_FOR_FILE.get(
            objectKey), Charset.defaultCharset()).toString();
        Assert.assertEquals(checksum, eventRecord.get("/" + WholeFileProcessedEvent.CHECKSUM).getValueAsString());
      }
    } finally {
      targetRunner.runDestroy();
    }
  }

  @Test
  public void testWholeFileInvalidRecord() throws Exception {
    AmazonS3Target amazonS3Target = createS3targetWithWholeFile();
    TargetRunner targetRunner = new TargetRunner.Builder(AmazonS3DTarget.class, amazonS3Target)
        .addService(DataFormatGeneratorService.class, new SdkWholeFileDataFormatGeneratorService())
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    targetRunner.runInit();
    try {
      Record invalidRecord = getRecords().get(0);
      invalidRecord.set(Field.create(new HashMap<>()));
      targetRunner.runWrite(ImmutableList.of(invalidRecord));
      Assert.assertEquals(1, targetRunner.getErrorRecords().size());
    } finally {
      targetRunner.runDestroy();
    }
  }

}
