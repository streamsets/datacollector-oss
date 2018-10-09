/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.transformer;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.io.fileref.LocalFileRef;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestWholeFileTransformerProcessor {
  static File notValidAvroFile;
  static File validAvroFile;
  static String rootPath;

  @BeforeClass
  public static void beforeTest() throws Exception {
    ClassLoader classLoader = TestWholeFileTransformerProcessor.class.getClassLoader();
    String notValidAvroFilePath = classLoader.getResource("file/not-valid-avro-file.avro").getFile();
    notValidAvroFile = new File(notValidAvroFilePath);

    String validAvroFilePath = classLoader.getResource("file/sdc-generated-sample.avro").getPath();
    validAvroFile = new File(validAvroFilePath);
    rootPath = notValidAvroFile.getParent();
  }

  @Test
  public void testEmptyDirTempConfig() throws StageException {
    Processor wholeFileTransofrmer  = new TestWholeFileTransformerProcessorBuilder()
        .tempDir("")
        .build();
    ProcessorRunner runner = new ProcessorRunner.Builder(WholeFileTransformerDProcessor.class, wholeFileTransofrmer)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(issues.size(), 1);
    Assert.assertTrue(issues.get(0).toString().contains(JobConfig.TEMPDIR));
  }

  @Test
  public void testNotWholeFileFormatRecord() throws StageException {
    Processor wholeFileTransofrmer  = new TestWholeFileTransformerProcessorBuilder()
        .tempDir("${file:parentPath(record:attribute('file'))}/.parquet")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(WholeFileTransformerDProcessor.class, wholeFileTransofrmer)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    Record record = RecordCreator.create();
    record.set(Field.create("test"));

    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      Assert.assertEquals(0, output.getRecords().get("a").size());

      List<Record> errorRecords = runner.getErrorRecords();
      Assert.assertEquals(1, errorRecords.size());
      Assert.assertEquals(Errors.CONVERT_01.getCode(), errorRecords.get(0).getHeader().getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidDirTempOnRuntime() throws Exception {
    Processor wholeFileTransofrmer  = new TestWholeFileTransformerProcessorBuilder()
        .tempDir(rootPath + "/.parquet")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(WholeFileTransformerDProcessor.class, wholeFileTransofrmer)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    Record record = RecordCreator.create();
    record.getHeader().setAttribute("size", "0");
    record.getHeader().setAttribute("file", "0");

    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("file/not-valid-avro-file.avro").getFile());

    record.set(
        FileRefUtil.getWholeFileRecordRootField(
            new LocalFileRef.Builder()
                .filePath(file.getPath())
                .bufferSize(1024)
                .verifyChecksum(false)
                .build(),
            getLocalFileMetadata(file.getPath())
        )
    );

    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      Map<String, List<Record>> outputRecord = output.getRecords();
      Assert.assertEquals(0, outputRecord.get("a").size());

      List<Record> errorRecords = runner.getErrorRecords();
      Assert.assertEquals(1, errorRecords.size());
      Assert.assertEquals(Errors.CONVERT_11.getCode(), errorRecords.get(0).getHeader().getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEmptyFileNameOnRuntime() throws Exception {
    Processor wholeFileTransofrmer  = new TestWholeFileTransformerProcessorBuilder()
        .tempDir("/tmp/out")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(WholeFileTransformerDProcessor.class, wholeFileTransofrmer)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    Record record = createRecord(notValidAvroFile);
    record.set(FileRefUtil.FILE_INFO_FIELD_PATH, Field.create(ImmutableMap.of("size", Field.create(10))));
    record.getHeader().setAttribute("size", "0");
    record.getHeader().setAttribute("file", "test");

    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      Map<String, List<Record>> outputRecord = output.getRecords();
      Assert.assertEquals(0, outputRecord.get("a").size());

      List<Record> errorRecords = runner.getErrorRecords();
      Assert.assertEquals(1, errorRecords.size());
      Assert.assertEquals(Errors.CONVERT_03.getCode(), errorRecords.get(0).getHeader().getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNotAbsoluteDirTempOnRuntime() throws Exception {
    Processor wholeFileTransofrmer  = new TestWholeFileTransformerProcessorBuilder()
        .tempDir("${file:parentPath(record:attribute('file'))}/.parquet")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(WholeFileTransformerDProcessor.class, wholeFileTransofrmer)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    Record record = createRecord(notValidAvroFile);
    record.getHeader().setAttribute("size", "0");
    record.getHeader().setAttribute("file", "./test");
    record.getHeader().setAttribute("filename", "test");

    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      Map<String, List<Record>> outputRecord = output.getRecords();
      Assert.assertEquals(0, outputRecord.get("a").size());

      List<Record> errorRecords = runner.getErrorRecords();
      Assert.assertEquals(1, errorRecords.size());
      Assert.assertEquals(Errors.CONVERT_05.getCode(), errorRecords.get(0).getHeader().getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSimpleWholeFileTransformerTest() throws Exception {
    Processor wholeFileTransofrmer  = new TestWholeFileTransformerProcessorBuilder()
        .tempDir(rootPath + "/.parquet")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(WholeFileTransformerDProcessor.class, wholeFileTransofrmer)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    Record record = createRecord(validAvroFile);

    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      Map<String, List<Record>> outputRecord = output.getRecords();
      Assert.assertEquals(1, outputRecord.get("a").size());
      Assert.assertTrue(outputRecord.get("a").get(0).get(FileRefUtil.FILE_INFO_FIELD_PATH) != null);
      Assert.assertTrue(outputRecord.get("a").get(0).get(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO_PATH) != null);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWrongInputFileValidationCheck() throws Exception {
    Processor wholeFileTransofrmer  = new TestWholeFileTransformerProcessorBuilder()
        .tempDir(rootPath + "/.parquet")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(WholeFileTransformerDProcessor.class, wholeFileTransofrmer)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    Record record = createRecord(notValidAvroFile);

    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      Map<String, List<Record>> outputRecord = output.getRecords();
      Assert.assertEquals(0, outputRecord.get("a").size());

      List<Record> errorRecords = runner.getErrorRecords();
      Assert.assertEquals(1, errorRecords.size());
      Assert.assertEquals(Errors.CONVERT_11.getCode(), errorRecords.get(0).getHeader().getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTempParquetSubDirectoryPath() throws Exception {
    final String tempSubDir = rootPath + "/subdir1/subsubdir1" + "/.parquet";
    WholeFileTransformerProcessor wholeFileTransofrmer  = new TestWholeFileTransformerProcessorBuilder()
        .tempDir(tempSubDir)
        .build();

    final Record record = createRecord(validAvroFile);
    final String sourceFileName = "testFile";

    ProcessorRunner runner = new ProcessorRunner.Builder(WholeFileTransformerDProcessor.class, wholeFileTransofrmer)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    try {
      runner.runInit();

      Path tempFilePath = wholeFileTransofrmer.getAndValidateTempFilePath(record, sourceFileName);
      Assert.assertEquals(tempSubDir, tempFilePath.getParent().toString());
      Assert.assertTrue(Files.exists(tempFilePath.getParent()));
    } finally {
      runner.runDestroy();
    }
  }

  private static Map<String, Object> getLocalFileMetadata(String filePath) throws Exception {
    String attributesToRead =
        Paths.get(filePath).getFileSystem().supportedFileAttributeViews().contains("posix")? "posix:*" : "*";
    Map<String, Object> metadata = new HashMap<>(Files.readAttributes(Paths.get(filePath), attributesToRead));
    metadata.put("filename", Paths.get(filePath).getFileName().toString());
    metadata.put("file", filePath);
    return metadata;
  }

  private static Record createRecord(File file) throws Exception {
    Record record = RecordCreator.create();
    record.set(
        FileRefUtil.getWholeFileRecordRootField(
            new LocalFileRef.Builder()
                .filePath(file.getPath())
                .bufferSize(1024)
                .verifyChecksum(false)
                .build(),
            getLocalFileMetadata(file.getPath())
        )
    );
    return record;
  }
}
