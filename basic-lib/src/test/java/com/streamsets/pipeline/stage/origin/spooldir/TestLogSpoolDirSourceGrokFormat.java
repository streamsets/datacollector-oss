/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Collections;
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
    return new SpoolDirSource(DataFormat.LOG, "UTF-8", false, 100, createTestDir(), 10, 1, "file-[0-9].log", 10, null, null,
      PostProcessingOptions.ARCHIVE, createTestDir(), 10, null, null, -1, null, 0, 0,
      null, 0, LogMode.GROK, 1000, true, null, null, Collections.<RegExConfig>emptyList(), GROK_PATTERN_DEFINITION,
      GROK_PATTERN, false, null, OnParseError.ERROR, 0, null);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    SpoolDirSource source = createSource();
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
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
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
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
