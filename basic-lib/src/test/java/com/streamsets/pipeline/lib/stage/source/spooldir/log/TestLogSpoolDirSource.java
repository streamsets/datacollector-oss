/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir.log;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.lib.stage.source.spooldir.FileDataType;
import com.streamsets.pipeline.lib.stage.source.spooldir.SpoolDirSource;
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

public class TestLogSpoolDirSource {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private final static String LINE1 = "1234567890";
  private final static String LINE2 = "A1234567890";

  private File createLogFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    IOUtils.write(LINE1 + "\n", writer);
    IOUtils.write(LINE2, writer);
    writer.close();
    return f;
  }

  @Test
  public void testProduceFullFile() throws Exception {
    SourceRunner runner = new SourceRunner.Builder(SpoolDirSource.class)
        .addConfiguration("postProcessing", DirectorySpooler.FilePostProcessing.ARCHIVE)
        .addConfiguration("filePattern", "file-[0-9].log")
        .addConfiguration("maxSpoolFiles", 10)
        .addConfiguration("spoolDir", createTestDir())
        .addConfiguration("archiveDir", createTestDir())
        .addConfiguration("retentionTimeMins", 10)
        .addConfiguration("initialFileToProcess", null)
        .addConfiguration("poolingTimeoutSecs", 0)
        .addConfiguration("errorArchiveDir", null)
        .addConfiguration("fileDataType", FileDataType.LOG_FILES)
        .addConfiguration("maxLogLineLength", 10)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      SpoolDirSource source = (SpoolDirSource) runner.getStage();
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      Assert.assertEquals(-1, source.produce(createLogFile(), 0, 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());
      Assert.assertEquals(LINE1, records.get(0).get().getValueAsMap().get("line").getValueAsString());
      Assert.assertEquals(false, records.get(0).get().getValueAsMap().get("truncated").getValueAsBoolean());
      Assert.assertEquals(LINE2.substring(0, 10), records.get(1).get().getValueAsMap().get("line").getValueAsString());
      Assert.assertEquals(true, records.get(1).get().getValueAsMap().get("truncated").getValueAsBoolean());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceLessThanFile() throws Exception {
    SourceRunner runner = new SourceRunner.Builder(SpoolDirSource.class)
        .addConfiguration("postProcessing", DirectorySpooler.FilePostProcessing.ARCHIVE)
        .addConfiguration("filePattern", "file-[0-9].log")
        .addConfiguration("maxSpoolFiles", 10)
        .addConfiguration("spoolDir", createTestDir())
        .addConfiguration("archiveDir", createTestDir())
        .addConfiguration("retentionTimeMins", 10)
        .addConfiguration("initialFileToProcess", null)
        .addConfiguration("poolingTimeoutSecs", 0)
        .addConfiguration("errorArchiveDir", null)
        .addConfiguration("fileDataType", FileDataType.LOG_FILES)
        .addConfiguration("maxLogLineLength", 10)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      SpoolDirSource source = (SpoolDirSource) runner.getStage();
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      long offset = source.produce(createLogFile(), 0, 1, batchMaker);
      Assert.assertEquals(11, offset);
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(LINE1, records.get(0).get().getValueAsMap().get("line").getValueAsString());
      Assert.assertEquals(false, records.get(0).get().getValueAsMap().get("truncated").getValueAsBoolean());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(createLogFile(), offset, 1, batchMaker);
      Assert.assertEquals(22, offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(LINE2.substring(0, 10), records.get(0).get().getValueAsMap().get("line").getValueAsString());
      Assert.assertEquals(true, records.get(0).get().getValueAsMap().get("truncated").getValueAsBoolean());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(createLogFile(), offset, 1, batchMaker);
      Assert.assertEquals(-1, offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(0, records.size());
    } finally {
      runner.runDestroy();
    }
  }

}
