/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
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

  private SpoolDirSource createSource(boolean truncated) {
    return new SpoolDirSource(DataFormat.TEXT, createTestDir(), 10, 0, "file-[0-9].log", 10, null, null,
                              PostProcessingOptions.ARCHIVE, createTestDir(), 10, null, false, false, null, 0, 10,
                              truncated, null, 0);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    SpoolDirSource source = createSource(false);
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      Assert.assertEquals(-1, source.produce(createLogFile(), 0, 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());
      Assert.assertEquals(LINE1, records.get(0).get().getValueAsMap().get("line").getValueAsString());
      Assert.assertNull(records.get(0).get().getValueAsMap().get("truncated"));
      Assert.assertEquals(LINE2.substring(0, 10), records.get(1).get().getValueAsMap().get("line").getValueAsString());
      Assert.assertNull(records.get(1).get().getValueAsMap().get("truncated"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceLessThanFile() throws Exception {
    SpoolDirSource source = createSource(true);
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane").build();
    runner.runInit();
    try {
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
