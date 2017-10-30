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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirRunnable;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestSDCRecordSpoolDirSource {
  private static final int threadNumber = 0;
  private static final int batchSize = 10;
  private static final Map<String, Offset> lastSourceOffset = new HashMap<>();

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private File createErrorRecordsFile() throws Exception {
    File f = new File(createTestDir(), "sdc-records-000000");
    Source.Context sourceContext = ContextInfoCreator.createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
      ImmutableList.of("lane"));

    FileOutputStream fileOutputStream = new FileOutputStream(f);
    RecordWriter jsonRecordWriter = ((ContextExtensions) sourceContext).createRecordWriter(fileOutputStream);

    Record r = RecordCreator.create("s", "c::1");
    r.set(Field.create("Hello"));
    jsonRecordWriter.write(r);
    r.set(Field.create("Bye"));
    jsonRecordWriter.write(r);
    jsonRecordWriter.close();
    return f;
  }

  private SpoolDirSource createSource(String dir) {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.SDC_JSON;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.overrunLimit = 100;
    conf.spoolDir = createTestDir();
    conf.batchSize = 10;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].log";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.ARCHIVE;
    conf.archiveDir = dir;
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    return new SpoolDirSource(conf);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    File errorRecordsFile = createErrorRecordsFile();
    SpoolDirSource source = createSource(errorRecordsFile.getParent());
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(errorRecordsFile, "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());
      Record r = RecordCreator.create("s", "c::1");
      r.set(Field.create("Hello"));
      Assert.assertEquals("Hello", records.get(0).get("/").getValueAsString());
      r.set(Field.create("Bye"));
      Assert.assertEquals("Bye", records.get(1).get("/").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

}
