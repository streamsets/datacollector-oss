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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.io.fileref.LocalFileRef;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsFileType;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsTarget;
import com.streamsets.pipeline.stage.destination.hdfs.LateRecordsAction;
import com.streamsets.pipeline.stage.destination.hdfs.util.HdfsTargetUtil;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriter;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriterManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.PowerMockUtils;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RunWith(PowerMockRunner.class)
@PrepareForTest({RecordWriterManager.class})
@PowerMockIgnore({"javax.*", "org.*"})
public class TestHDFSTargetWholeFile {
  private String testDir;
  private URI uri;

  @Before
  public void before() {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    testDir = dir.getAbsolutePath();
    try {
      uri = new URI("file:///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    System.setProperty("HADOOP_USER_NAME", System.getProperty("user.name"));
  }

  private String getTestDir() {
    return testDir;
  }

  @Test(expected = StageException.class)
  public void testInvalidMaxRecordsPerFileOnWholeFileFormat() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
        .dirPathTemplate(getTestDir() + "/hdfs/${record:attribute('key')}/a/b/c}")
        .timeDriver("${time:now()}")
        .dataForamt(DataFormat.WHOLE_FILE)
        .fileNameEL("${record:value('/fileInfo/fileName')}")
        .maxFileSize(2)
        .maxRecordsPerFile(1)
        .fileType(HdfsFileType.WHOLE_FILE)
        .idleTimeout("-1")
        .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
        .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();
  }

  @Test(expected = StageException.class)
  public void testInvalidMaxFileSizeOnWholeFileFormat() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
        .dirPathTemplate(getTestDir() + "/hdfs/${record:attribute('key')}/a/b/c}")
        .timeDriver("${time:now()}")
        .dataForamt(DataFormat.WHOLE_FILE)
        .fileNameEL("${record:value('/fileInfo/fileName')}")
        .maxRecordsPerFile(2)
        .maxFileSize(0)
        .fileType(HdfsFileType.WHOLE_FILE)
        .idleTimeout("-1")
        .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
        .hdfsUri(uri.toString())
        .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    runner.runInit();
  }

  @Test(expected = StageException.class)
  public void testInvalidFileTypeOnWholeFileFormat() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
        .dirPathTemplate(getTestDir() + "/hdfs/${record:attribute('key')}/a/b/c}")
        .timeDriver("${time:now()}")
        .dataForamt(DataFormat.WHOLE_FILE)
        .fileNameEL("${record:value('/fileInfo/fileName')}")
        .maxFileSize(0)
        .maxRecordsPerFile(1)
        .idleTimeout("-1")
        .fileType(HdfsFileType.TEXT)
        .hdfsUri(uri.toString())
        .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
        .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();
  }

  @Test(expected = StageException.class)
  public void testInvalidIdleTimeoutOnWholeFileFormat() throws Exception {
    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
        .dirPathTemplate(getTestDir() + "/hdfs/${record:attribute('key')}/a/b/c}")
        .timeDriver("${time:now()}")
        .dataForamt(DataFormat.WHOLE_FILE)
        .fileNameEL("${record:value('/fileInfo/fileName')}")
        .maxFileSize(0)
        .maxRecordsPerFile(1)
        .fileType(HdfsFileType.TEXT)
        .hdfsUri(uri.toString())
        .idleTimeout("{1 * HOURS}")
        .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
        .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();
    runner.runInit();
  }

  @Test
  public void testWholeFileCopyMultipleFiles() throws Exception {
    java.nio.file.Path filePath1 = Paths.get(getTestDir() + "/source_testWholeFileCopyMultipleFiles1.txt");
    java.nio.file.Path filePath2 = Paths.get(getTestDir() + "/source_testWholeFileCopyMultipleFiles2.txt");

    Files.write(filePath1, "This is a sample file 1 with some text".getBytes());
    Files.write(filePath2, "This is a sample file 2 with some text".getBytes());

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
        .hdfsUri(uri.toString())
        .dirPathTemplate(getTestDir())
        .timeDriver("${time:now()}")
        .dataForamt(DataFormat.WHOLE_FILE)
        .fileType(HdfsFileType.WHOLE_FILE)
        .fileNameEL("${record:value('/fileInfo/filename')}")
        .maxRecordsPerFile(1)
        .maxFileSize(0)
        .uniquePrefix("sdc-")
        .idleTimeout("-1")
        .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
        .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    runner.runInit();
    runner.runWrite(Arrays.asList(getFileRefRecordForFile(filePath1), getFileRefRecordForFile(filePath2)));

    for (Path filePath : Arrays.asList(filePath1, filePath2)) {
      String targetFileName = getTestDir() + "/" + "sdc-" + filePath.getFileName();

      //Now check the file is copied as a whole.
      Assert.assertTrue(Files.exists(Paths.get(targetFileName)));

      try (InputStream is1 = new FileInputStream(filePath.toString());
           InputStream is2 = new FileInputStream(targetFileName)) {
        checkFileContent(is1, is2);
      }
    }
    runner.runDestroy();
  }

  @Test
  public void testWholeFilePartialCopy() throws Exception {
    PowerMockito.replace(
        MemberMatcher.method(RecordWriterManager.class, "commitWriter", RecordWriter.class)
    ).with(new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        //Close the recordWriter so contents are written.
        RecordWriter recordWriter = (RecordWriter)args[0];
        recordWriter.close();
        //Make it no op so that the _tmp_ file is left behind
        return null;
      }
    });

    java.nio.file.Path filePath = Paths.get(getTestDir() + "/source_testWholeFilePartialCopy.txt");
    Files.write(filePath, "This is a sample file with some text".getBytes());

    HdfsTarget hdfsTarget = HdfsTargetUtil.newBuilder()
        .hdfsUri(uri.toString())
        .dirPathTemplate(getTestDir())
        .timeDriver("${time:now()}")
        .dataForamt(DataFormat.WHOLE_FILE)
        .fileType(HdfsFileType.WHOLE_FILE)
        .fileNameEL("${record:value('/fileInfo/filename')}")
        .maxRecordsPerFile(1)
        .maxFileSize(0)
        .uniquePrefix("sdc-")
        .idleTimeout("-1")
        .lateRecordsAction(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE)
        .build();

    TargetRunner runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    Record record = getFileRefRecordForFile(filePath);

    runner.runInit();
    runner.runWrite(Collections.singletonList(record));
    runner.runDestroy();

    runner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .build();

    final String tmpTargetFileName = getTestDir() + "/" + "_tmp_sdc-" + filePath.getFileName();
    final String targetFileName = getTestDir() + "/" + "sdc-" + filePath.getFileName();

    //Make sure _tmp_ file is there
    Assert.assertTrue(Files.exists(Paths.get(tmpTargetFileName)));
    Assert.assertTrue(Files.size(Paths.get(tmpTargetFileName)) > 0);

    //Make sure the file is not renamed to the final file.
    Assert.assertFalse(Files.exists(Paths.get(targetFileName)));

    PowerMockito.replace(
        MemberMatcher.method(RecordWriterManager.class, "commitWriter", RecordWriter.class)
    ).with(new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        //Set the commit Writer back.
        return method.invoke(proxy, args);
      }
    });

    runner.runInit();
    runner.runWrite(Collections.singletonList(record));

    //Make sure _tmp_ file is not there
    Assert.assertFalse(Files.exists(Paths.get(tmpTargetFileName)));

    //Make sure the file is renamed to the final file.
    Assert.assertTrue(Files.exists(Paths.get(targetFileName)));
    Assert.assertTrue(Files.size(Paths.get(targetFileName)) > 0);
    checkFileContent(new FileInputStream(filePath.toString()), new FileInputStream(targetFileName));
    runner.runDestroy();
  }

  private Record getFileRefRecordForFile(Path filePath) throws Exception {
    Record fileRefRecord = RecordCreator.create();
    FileRef fileRef =
        new LocalFileRef.Builder()
            .bufferSize(1024)
            .createMetrics(false)
            .verifyChecksum(false)
            .filePath(filePath.toAbsolutePath().toString())
            .build();
    Map<String, Field> fieldMap = new HashMap<>();

    Map<String, Object> metadata = new HashMap<>(Files.readAttributes(filePath, "*"));
    metadata.put("filename", filePath.getFileName());
    metadata.put("file", filePath.toString());
    metadata.put("dir", filePath.getParent().toString());

    fieldMap.put("fileRef", Field.create(Field.Type.FILE_REF, fileRef));
    fieldMap.put("fileInfo", createFieldForMetadata(metadata));
    fileRefRecord.set(Field.create(fieldMap));

    return fileRefRecord;
  }

  private void checkFileContent(InputStream is1, InputStream is2) throws Exception {
    int totalBytesRead1 = 0, totalBytesRead2 = 0;
    int a = 0, b = 0;
    while (a != -1 || b != -1) {
      totalBytesRead1 = ((a = is1.read()) != -1)? totalBytesRead1 + 1 : totalBytesRead1;
      totalBytesRead2 = ((b = is2.read()) != -1)? totalBytesRead2 + 1 : totalBytesRead2;
      Assert.assertEquals(a, b);
    }
    Assert.assertEquals(totalBytesRead1, totalBytesRead2);
  }

  private static Field createFieldForMetadata(Object metadataObject) {
    if (metadataObject instanceof Boolean) {
      return Field.create((Boolean) metadataObject);
    } else if (metadataObject instanceof Character) {
      return Field.create((Character) metadataObject);
    } else if (metadataObject instanceof Byte) {
      return Field.create((Byte) metadataObject);
    } else if (metadataObject instanceof Short) {
      return Field.create((Short) metadataObject);
    } else if (metadataObject instanceof Integer) {
      return Field.create((Integer) metadataObject);
    } else if (metadataObject instanceof Long) {
      return Field.create((Long) metadataObject);
    } else if (metadataObject instanceof Float) {
      return Field.create((Float) metadataObject);
    } else if (metadataObject instanceof Double) {
      return Field.create((Double) metadataObject);
    } else if (metadataObject instanceof Date) {
      return Field.createDatetime((Date) metadataObject);
    } else if (metadataObject instanceof BigDecimal) {
      return Field.create((BigDecimal) metadataObject);
    } else if (metadataObject instanceof String) {
      return Field.create((String) metadataObject);
    } else if (metadataObject instanceof byte[]) {
      return Field.create((byte[]) metadataObject);
    } else if (metadataObject instanceof Collection) {
      Iterator iterator = ((Collection)metadataObject).iterator();
      List<Field> fields = new ArrayList<>();
      while (iterator.hasNext()) {
        fields.add(createFieldForMetadata(iterator.next()));
      }
      return Field.create(fields);
    } else if (metadataObject instanceof Map) {
      boolean isListMap = (metadataObject instanceof LinkedHashMap);
      Map<String, Field> fieldMap = isListMap? new LinkedHashMap<String, Field>() : new HashMap<String, Field>();
      Map map = (Map)metadataObject;
      for (Object key : map.keySet()) {
        fieldMap.put(key.toString(), createFieldForMetadata(map.get(key)));
      }
      return isListMap? Field.create(Field.Type.LIST_MAP, fieldMap) : Field.create(fieldMap);
    } else {
      return Field.create(metadataObject.toString());
    }
  }
}
