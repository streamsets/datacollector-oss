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

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.List;
import java.util.UUID;

public class TestLogSpoolDirSourceGrokFormat {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private static final String LINE1 = "[3223] 26 Feb 23:59:01 Background append only file rewriting started by pid " +
    "19383 [19383] 26 Feb 23:59:01 SYNC append only file rewrite performed ";

  private static final String LINE2 = "[3223] 26 Mar 23:59:01 Background append only file rewriting started by pid " +
    "19383 [19383] 26 Feb 23:59:01 SYNC append only file rewrite performed ";

  private static final String GROK_PATTERN_DEFINITION =
    "REDISTIMESTAMP %{MONTHDAY} %{MONTH} %{TIME}\n" +
      "REDISLOG \\[%{POSINT:pid}\\] %{REDISTIMESTAMP:timestamp} %{GREEDYDATA:message}";

  private static final String GROK_PATTERN = "%{REDISLOG}";

  private File createLogFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    IOUtils.write(LINE1 + "\n", writer);
    IOUtils.write(LINE2, writer);
    writer.close();
    return f;
  }

  private SpoolDirSource createSource() {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.LOG;
    conf.spoolDir = createTestDir();
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].log";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.ARCHIVE;
    conf.archiveDir = createTestDir();
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.logMode = LogMode.GROK;
    conf.dataFormatConfig.logMaxObjectLen = 1000;
    conf.dataFormatConfig.retainOriginalLine = true;
    conf.dataFormatConfig.grokPattern = GROK_PATTERN;
    conf.dataFormatConfig.grokPatternDefinition = GROK_PATTERN_DEFINITION;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    return new SpoolDirSource(conf);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    SpoolDirSource source = createSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      Assert.assertEquals("-1", source.produce(createLogFile(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());

      Assert.assertFalse(records.get(0).has("/truncated"));

      Record record = records.get(0);

      Assert.assertEquals(LINE1, record.get().getValueAsMap().get("originalLine").getValueAsString());

      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/timestamp"));
      Assert.assertEquals("26 Feb 23:59:01", record.get("/timestamp").getValueAsString());

      Assert.assertTrue(record.has("/pid"));
      Assert.assertEquals("3223", record.get("/pid").getValueAsString());

      Assert.assertTrue(record.has("/message"));
      Assert.assertEquals("Background append only file rewriting started by pid " +
        "19383 [19383] 26 Feb 23:59:01 SYNC append only file rewrite performed ",
        record.get("/message").getValueAsString());


      record = records.get(1);

      Assert.assertEquals(LINE2, records.get(1).get().getValueAsMap().get("originalLine").getValueAsString());
      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/timestamp"));
      Assert.assertEquals("26 Mar 23:59:01", record.get("/timestamp").getValueAsString());

      Assert.assertTrue(record.has("/pid"));
      Assert.assertEquals("3223", record.get("/pid").getValueAsString());

      Assert.assertTrue(record.has("/message"));
      Assert.assertEquals("Background append only file rewriting started by pid " +
          "19383 [19383] 26 Feb 23:59:01 SYNC append only file rewrite performed ",
        record.get("/message").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceLessThanFile() throws Exception {
    SpoolDirSource source = createSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      String offset = source.produce(createLogFile(), "0", 1, batchMaker);

      Assert.assertEquals("147", offset);
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());


      Record record = records.get(0);
      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/timestamp"));
      Assert.assertEquals("26 Feb 23:59:01", record.get("/timestamp").getValueAsString());

      Assert.assertTrue(record.has("/pid"));
      Assert.assertEquals("3223", record.get("/pid").getValueAsString());

      Assert.assertTrue(record.has("/message"));
      Assert.assertEquals("Background append only file rewriting started by pid " +
          "19383 [19383] 26 Feb 23:59:01 SYNC append only file rewrite performed ",
        record.get("/message").getValueAsString());


      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(createLogFile(), offset, 1, batchMaker);
      Assert.assertEquals("293", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());

      Assert.assertEquals(LINE2, records.get(0).get().getValueAsMap().get("originalLine").getValueAsString());
      Assert.assertFalse(records.get(0).has("/truncated"));

      record = records.get(0);

      Assert.assertTrue(record.has("/timestamp"));
      Assert.assertEquals("26 Mar 23:59:01", record.get("/timestamp").getValueAsString());

      Assert.assertTrue(record.has("/pid"));
      Assert.assertEquals("3223", record.get("/pid").getValueAsString());

      Assert.assertTrue(record.has("/message"));
      Assert.assertEquals("Background append only file rewriting started by pid " +
          "19383 [19383] 26 Feb 23:59:01 SYNC append only file rewrite performed ",
        record.get("/message").getValueAsString());


      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(createLogFile(), offset, 1, batchMaker);
      Assert.assertEquals("-1", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(0, records.size());

    } finally {
      runner.runDestroy();
    }
  }
}
