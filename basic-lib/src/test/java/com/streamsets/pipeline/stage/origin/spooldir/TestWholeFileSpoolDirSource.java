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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirRunnable;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class TestWholeFileSpoolDirSource {
  private String testDir;

  @Before
  public void createTestDir() throws Exception {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    testDir = f.getAbsolutePath();

  }

  private SpoolDirSource createSource() {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.WHOLE_FILE;
    conf.spoolDir = testDir;
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "*";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.wholeFileMaxObjectLen = 1024;
    return new SpoolDirSource(conf);
  }

  @Test
  public void testWholeFileRecordsForFile() throws Exception {
    Path sourcePath = Paths.get(testDir + "/source.txt");
    Files.write(sourcePath, "Sample Text 1".getBytes());
    Files.setAttribute(
        sourcePath,
        "posix:permissions",
        ImmutableSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE,
            PosixFilePermission.GROUP_READ
        )
    );

    SpoolDirSource source = createSource();
    PushSourceRunner runner =
        new PushSourceRunner.Builder(SpoolDirDSource.class, source)
            .addOutputLane("lane")
            .setOnRecordError(OnRecordError.TO_ERROR)
            .build();

    final List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
    AtomicInteger batchCount = new AtomicInteger(0);

    runner.runInit();
    try {
      runner.runProduce(new HashMap<>(), 10, output2 -> {
        synchronized (records) {
          records.addAll(output2.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();
        runner.setStop();
      });

      runner.waitOnProduce();

      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Record record = records.get(0);

      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH));
      Assert.assertTrue(record.has(FileRefUtil.FILE_REF_FIELD_PATH));

      Assert.assertEquals(Field.Type.FILE_REF, record.get(FileRefUtil.FILE_REF_FIELD_PATH).getType());
      Assert.assertEquals(Field.Type.MAP, record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getType());

      Map<String, Object> metadata = Files.readAttributes(sourcePath, "posix:*");
      Assert.assertTrue(record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getValueAsMap().keySet().containsAll(metadata.keySet()));

      //Check permissions
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + SpoolDirRunnable.PERMISSIONS));
      Assert.assertEquals(
          "rwxr-----",
          record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + SpoolDirRunnable.PERMISSIONS).getValueAsString()
      );

      Assert.assertEquals(Field.Type.FILE_REF, record.get(FileRefUtil.FILE_REF_FIELD_PATH).getType());
      Assert.assertEquals(Field.Type.MAP, record.get(FileRefUtil.FILE_INFO_FIELD_PATH).getType());

    } finally {
      runner.runDestroy();
    }
  }


  private void initMetrics(Stage.Context context) {
    context.createMeter(FileRefUtil.TRANSFER_THROUGHPUT_METER);
    final Map<String, Object> gaugeStatistics = context.createGauge(FileRefUtil.GAUGE_NAME).getValue();
    gaugeStatistics.put(FileRefUtil.TRANSFER_THROUGHPUT, 0L);
    gaugeStatistics.put(FileRefUtil.SENT_BYTES, 0L);
    gaugeStatistics.put(FileRefUtil.REMAINING_BYTES, 0L);
    gaugeStatistics.put(FileRefUtil.COMPLETED_FILE_COUNT, 0L);
  }


  @Test
  public void testWholeFileRecordsCopy() throws Exception {
    Path sourcePath = Paths.get(testDir + "/source.txt");
    Files.write(sourcePath, "Sample Text 1".getBytes());
    SpoolDirSource source = createSource();
    PushSourceRunner runner =
        new PushSourceRunner.Builder(SpoolDirDSource.class, source)
            .addOutputLane("lane")
            .setOnRecordError(OnRecordError.TO_ERROR)
            .build();

    final List<Record> records = Collections.synchronizedList(new ArrayList<>(10));
    AtomicInteger batchCount = new AtomicInteger(0);

    runner.runInit();

    try {
      runner.runProduce(new HashMap<>(), 10, output2 -> {
        synchronized (records) {
          records.addAll(output2.getRecords().get("lane"));
        }
        batchCount.incrementAndGet();
        runner.setStop();
      });
      runner.waitOnProduce();

      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Record record = records.get(0);
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH));
      Assert.assertTrue(record.has(FileRefUtil.FILE_REF_FIELD_PATH));

      FileRef fileRef = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef();
      String targetFile = testDir + "/target.txt";
      Stage.Context context = (Stage.Context) Whitebox.getInternalState(source, "context");

      initMetrics(context);

      IOUtils.copy(
          fileRef.createInputStream(context, InputStream.class),
          new FileOutputStream(targetFile)
      );
      //Now make sure the file is copied properly,
      checkFileContent(new FileInputStream(sourcePath.toString()), new FileInputStream(targetFile));
    } finally {
      runner.runDestroy();
    }
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

}
