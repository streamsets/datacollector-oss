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
package com.streamsets.pipeline.lib.generator.wholefile;

import com.google.common.io.Files;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.ChecksumAlgorithm;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.hashing.HashingUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefStreamCloseEventHandler;
import com.streamsets.pipeline.lib.io.fileref.FileRefTestUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class TestWholeFileDataGenerator {
  private File testDir;
  private Stage.Context context;

  @Before
  public void setup() throws Exception {
    testDir = new File("target", UUID.randomUUID().toString());
    testDir.mkdirs();
    FileRefTestUtil.writePredefinedTextToFile(testDir);
    context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
  }

  @After
  public void tearDown() throws Exception {
    testDir.delete();
  }

  private String getTargetFilePath() {
    return testDir.getAbsolutePath() + "/target.txt";
  }

  private Record createRecord(String checksum, HashingUtil.HashType checksumAlgorithm) throws Exception {
    Record record = context.createRecord("id");
    Map<String, Object> metadata = FileRefTestUtil.getFileMetadata(testDir);
    FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, true, checksum, checksumAlgorithm);
    record.set(FileRefUtil.getWholeFileRecordRootField(fileRef, metadata));
    return record;
  }

  @Test
  public void testFileTransfer() throws Exception {
    OutputStream os = new FileOutputStream(getTargetFilePath());
    DataGeneratorFactory factory =
        new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.WHOLE_FILE).build();
    try (DataGenerator gen = factory.getGenerator(os)) {
      gen.write(createRecord(null, null));
    }
    InputStream targetFileInputStream = new FileInputStream(getTargetFilePath());
    byte[] b = new byte[FileRefTestUtil.TEXT.getBytes().length];
    int bytesRead = targetFileInputStream.read(b, 0 , b.length);
    Assert.assertEquals(FileRefTestUtil.TEXT.getBytes().length, bytesRead);
    Assert.assertArrayEquals(FileRefTestUtil.TEXT.getBytes(), b);
    targetFileInputStream.close();
  }

  private void testInvalidRecord(String fieldPathToBeRemoved) throws Exception {
    Record record = createRecord(null, null);
    record.delete(fieldPathToBeRemoved);
    OutputStream os = new FileOutputStream(getTargetFilePath());
    DataGeneratorFactory factory =
        new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.WHOLE_FILE).build();
    try (DataGenerator gen = factory.getGenerator(os)) {
      gen.write(record);
    } catch (DataGeneratorException e) {
      Assert.assertEquals(Errors.WHOLE_FILE_GENERATOR_ERROR_0, e.getErrorCode());
    }
  }

  @Test
  public void testInvalidRecordWithNoFileRef() throws Exception {
    testInvalidRecord(FileRefUtil.FILE_REF_FIELD_PATH);
  }

  @Test
  public void testInvalidRecordWithNoFileInfo() throws Exception {
    testInvalidRecord(FileRefUtil.FILE_INFO_FIELD_PATH);
  }

  @Test
  public void testFileTransferProperChecksum() throws Exception {
    OutputStream os = new FileOutputStream(getTargetFilePath());
    DataGeneratorFactory factory =
        new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.WHOLE_FILE).build();
    try (DataGenerator gen = factory.getGenerator(os)) {
      gen.write(
          createRecord(
              Files.hash(
                  new File(FileRefTestUtil.getSourceFilePath(testDir)),
                  HashingUtil.getHasher(HashingUtil.HashType.MD5)
              ).toString(),
              HashingUtil.HashType.MD5
          )
      );
    }
    try (InputStream targetFileInputStream = new FileInputStream(getTargetFilePath())) {
      byte[] b = new byte[FileRefTestUtil.TEXT.getBytes().length];
      int bytesRead = targetFileInputStream.read(b, 0, b.length);
      Assert.assertEquals(FileRefTestUtil.TEXT.getBytes().length, bytesRead);
      Assert.assertArrayEquals(FileRefTestUtil.TEXT.getBytes(), b);
    }
  }


  @Test(expected = IOException.class)
  public void testFileTransferInvalidChecksum() throws Exception {
    OutputStream os = new FileOutputStream(getTargetFilePath());
    DataGeneratorFactory factory =
        new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.WHOLE_FILE).build();
    try (DataGenerator gen = factory.getGenerator(os)) {
      Record record = createRecord(
          Files.hash(
              new File(FileRefTestUtil.getSourceFilePath(testDir)),
              HashingUtil.getHasher(HashingUtil.HashType.MD5)
          ).toString(),
          //Setting it to CRC32 instead of MD5 to induce a wrong checksum
          HashingUtil.HashType.CRC32
      );
      gen.write(record);
    }
  }

  @Test
  public void testGeneratorWithStreamHandler() throws Exception {
    Record eventRecord = WholeFileProcessedEvent.FILE_TRANSFER_COMPLETE_EVENT.create(context, null)
        .with(WholeFileProcessedEvent.SOURCE_FILE_INFO, Collections.emptyMap())
        .withStringMap(WholeFileProcessedEvent.TARGET_FILE_INFO, Collections.emptyMap())
        .create();

    OutputStream os = new FileOutputStream(getTargetFilePath());
    DataGeneratorFactory factory =
        new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.WHOLE_FILE)
            .setConfig(WholeFileDataGeneratorFactory.INCLUDE_CHECKSUM_IN_THE_EVENTS_KEY, true)
            .setConfig(WholeFileDataGeneratorFactory.CHECKSUM_ALGO_KEY, ChecksumAlgorithm.MD5)
            .build();

    try (DataGenerator gen = factory.getGenerator(os, new FileRefStreamCloseEventHandler(eventRecord))) {
      Record record = createRecord(null, null);
      gen.write(record);
    }
    Assert.assertTrue(eventRecord.has(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO_PATH));
    Assert.assertTrue(eventRecord.has(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO_PATH));
    Assert.assertTrue(eventRecord.has("/"+ WholeFileProcessedEvent.CHECKSUM_ALGORITHM));
    Assert.assertTrue(eventRecord.has("/" + WholeFileProcessedEvent.CHECKSUM));
  }
}
