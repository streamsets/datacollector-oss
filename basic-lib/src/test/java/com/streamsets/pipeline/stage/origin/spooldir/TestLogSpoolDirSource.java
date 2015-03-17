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

  private final static String LINE1 = "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326";
  private final static String LINE2 = "127.0.0.2 ss m [10/Oct/2000:13:55:36 -0800] \"GET /apache_pb.gif HTTP/2.0\" 200 2326";

  private File createLogFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    IOUtils.write(LINE1 + "\n", writer);
    IOUtils.write(LINE2, writer);
    writer.close();
    return f;
  }

  private SpoolDirSource createSource() {
    return new SpoolDirSource(DataFormat.LOG, "UTF-8", 100, createTestDir(), 10, 1, "file-[0-9].log", 10, null, null,
      PostProcessingOptions.ARCHIVE, createTestDir(), 10, null, null, -1, null, 0, 0,
      null, 0, LogMode.COMMON_LOG_FORMAT, 100, true);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    SpoolDirSource source = createSource();
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      Assert.assertEquals(-1, source.produce(createLogFile(), 0, 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());
      Assert.assertEquals(LINE1, records.get(0).get().getValueAsMap().get("text").getValueAsString());
      Assert.assertFalse(records.get(0).has("/truncated"));

      Record record = records.get(0);
      Assert.assertTrue(record.has("/ipAddress"));
      Assert.assertEquals("127.0.0.1", record.get("/ipAddress").getValueAsString());

      Assert.assertTrue(record.has("/clientId"));
      Assert.assertEquals("ss", record.get("/clientId").getValueAsString());

      Assert.assertTrue(record.has("/userId"));
      Assert.assertEquals("h", record.get("/userId").getValueAsString());

      Assert.assertTrue(record.has("/dateTime"));
      Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/dateTime").getValueAsString());

      Assert.assertTrue(record.has("/method"));
      Assert.assertEquals("GET", record.get("/method").getValueAsString());

      Assert.assertTrue(record.has("/request"));
      Assert.assertEquals("/apache_pb.gif", record.get("/request").getValueAsString());

      Assert.assertTrue(record.has("/protocol"));
      Assert.assertEquals("HTTP/1.0", record.get("/protocol").getValueAsString());

      Assert.assertTrue(record.has("/responseCode"));
      Assert.assertEquals("200", record.get("/responseCode").getValueAsString());

      Assert.assertTrue(record.has("/size"));
      Assert.assertEquals("2326", record.get("/size").getValueAsString());

      Assert.assertEquals(LINE2, records.get(1).get().getValueAsMap().get("text").getValueAsString());
      Assert.assertFalse(records.get(1).has("/truncated"));

      record = records.get(1);
      Assert.assertTrue(record.has("/ipAddress"));
      Assert.assertEquals("127.0.0.2", record.get("/ipAddress").getValueAsString());

      Assert.assertTrue(record.has("/clientId"));
      Assert.assertEquals("ss", record.get("/clientId").getValueAsString());

      Assert.assertTrue(record.has("/userId"));
      Assert.assertEquals("m", record.get("/userId").getValueAsString());

      Assert.assertTrue(record.has("/dateTime"));
      Assert.assertEquals("10/Oct/2000:13:55:36 -0800", record.get("/dateTime").getValueAsString());

      Assert.assertTrue(record.has("/method"));
      Assert.assertEquals("GET", record.get("/method").getValueAsString());

      Assert.assertTrue(record.has("/request"));
      Assert.assertEquals("/apache_pb.gif", record.get("/request").getValueAsString());

      Assert.assertTrue(record.has("/protocol"));
      Assert.assertEquals("HTTP/2.0", record.get("/protocol").getValueAsString());

      Assert.assertTrue(record.has("/responseCode"));
      Assert.assertEquals("200", record.get("/responseCode").getValueAsString());

      Assert.assertTrue(record.has("/size"));
      Assert.assertEquals("2326", record.get("/size").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceLessThanFile() throws Exception {
    SpoolDirSource source = createSource();
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      long offset = source.produce(createLogFile(), 0, 1, batchMaker);
      //FIXME
      Assert.assertEquals(83, offset);
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());


      Record record = records.get(0);
      Assert.assertTrue(record.has("/ipAddress"));
      Assert.assertEquals("127.0.0.1", record.get("/ipAddress").getValueAsString());

      Assert.assertTrue(record.has("/clientId"));
      Assert.assertEquals("ss", record.get("/clientId").getValueAsString());

      Assert.assertTrue(record.has("/userId"));
      Assert.assertEquals("h", record.get("/userId").getValueAsString());

      Assert.assertTrue(record.has("/dateTime"));
      Assert.assertEquals("10/Oct/2000:13:55:36 -0700", record.get("/dateTime").getValueAsString());

      Assert.assertTrue(record.has("/method"));
      Assert.assertEquals("GET", record.get("/method").getValueAsString());

      Assert.assertTrue(record.has("/request"));
      Assert.assertEquals("/apache_pb.gif", record.get("/request").getValueAsString());

      Assert.assertTrue(record.has("/protocol"));
      Assert.assertEquals("HTTP/1.0", record.get("/protocol").getValueAsString());

      Assert.assertTrue(record.has("/responseCode"));
      Assert.assertEquals("200", record.get("/responseCode").getValueAsString());

      Assert.assertTrue(record.has("/size"));
      Assert.assertEquals("2326", record.get("/size").getValueAsString());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = source.produce(createLogFile(), offset, 1, batchMaker);
      Assert.assertEquals(165, offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());

      Assert.assertEquals(LINE2, records.get(0).get().getValueAsMap().get("text").getValueAsString());
      Assert.assertFalse(records.get(0).has("/truncated"));

      record = records.get(0);
      Assert.assertTrue(record.has("/ipAddress"));
      Assert.assertEquals("127.0.0.2", record.get("/ipAddress").getValueAsString());

      Assert.assertTrue(record.has("/clientId"));
      Assert.assertEquals("ss", record.get("/clientId").getValueAsString());

      Assert.assertTrue(record.has("/userId"));
      Assert.assertEquals("m", record.get("/userId").getValueAsString());

      Assert.assertTrue(record.has("/dateTime"));
      Assert.assertEquals("10/Oct/2000:13:55:36 -0800", record.get("/dateTime").getValueAsString());

      Assert.assertTrue(record.has("/method"));
      Assert.assertEquals("GET", record.get("/method").getValueAsString());

      Assert.assertTrue(record.has("/request"));
      Assert.assertEquals("/apache_pb.gif", record.get("/request").getValueAsString());

      Assert.assertTrue(record.has("/protocol"));
      Assert.assertEquals("HTTP/2.0", record.get("/protocol").getValueAsString());

      Assert.assertTrue(record.has("/responseCode"));
      Assert.assertEquals("200", record.get("/responseCode").getValueAsString());

      Assert.assertTrue(record.has("/size"));
      Assert.assertEquals("2326", record.get("/size").getValueAsString());


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
