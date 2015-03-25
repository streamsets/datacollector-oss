/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.List;
import java.util.UUID;

public class TestCsvSpoolDirSource {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private final static String LINE1 = "A,B";
  private final static String LINE2 = "a,b";
  private final static String LINE3 = "e,f";

  private File createLogFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    writer.write(LINE1 + "\n");
    writer.write(LINE2 + "\n");
    writer.write(LINE3 + "\n");
    writer.close();
    return f;
  }

  private SpoolDirSource createSource(CsvHeader header) {
    return new SpoolDirSource(DataFormat.DELIMITED, "UTF-8",100, createTestDir(), 10, 1, "file-[0-9].log", 10, null, null,
                              PostProcessingOptions.ARCHIVE, createTestDir(), 10, CsvMode.RFC4180, header, 5, null, 0,
                              10, null, 0, null, 0, false, null, null, null, null, null, false, null);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    SpoolDirSource source = createSource(CsvHeader.NO_HEADER);
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      Assert.assertEquals(-1, source.produce(createLogFile(), 0, 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(3, records.size());
      Assert.assertEquals("A", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("B", records.get(0).get("[1]/value").getValueAsString());
      Assert.assertFalse(records.get(0).has("[0]/header"));
      Assert.assertFalse(records.get(0).has("[1]/header"));
      Assert.assertFalse(records.get(0).has("[2]"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceLessThanFileIgnoreHeader() throws Exception {
    testProduceLessThanFile(true);
  }

  @Test
  public void testProduceLessThanFileWithHeader() throws Exception {
    testProduceLessThanFile(false);
  }

  private void testProduceLessThanFile(boolean ignoreHeader) throws Exception {
    SpoolDirSource source = createSource((ignoreHeader) ? CsvHeader.IGNORE_HEADER : CsvHeader.WITH_HEADER);
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      long offset = source.produce(createLogFile(), 0, 1, batchMaker);
      Assert.assertEquals(8, offset);
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("a", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("b", records.get(0).get("[1]/value").getValueAsString());
      if (ignoreHeader) {
        Assert.assertFalse(records.get(0).has("[0]/header"));
        Assert.assertFalse(records.get(0).has("[1]/header"));
      } else {
        Assert.assertEquals("A", records.get(0).get("[0]/header").getValueAsString());
        Assert.assertEquals("B", records.get(0).get("[1]/header").getValueAsString());
      }

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(createLogFile(), offset, 1, batchMaker);
      Assert.assertEquals(12, offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("e", records.get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("f", records.get(0).get("[1]/value").getValueAsString());
      if (ignoreHeader) {
        Assert.assertFalse(records.get(0).has("[0]/header"));
        Assert.assertFalse(records.get(0).has("[1]/header"));
      } else {
        Assert.assertEquals("A", records.get(0).get("[0]/header").getValueAsString());
        Assert.assertEquals("B", records.get(0).get("[1]/header").getValueAsString());
      }

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
