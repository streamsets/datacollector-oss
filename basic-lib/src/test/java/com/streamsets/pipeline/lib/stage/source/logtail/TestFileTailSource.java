/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.logtail;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

public class TestFileTailSource {

  @Test(expected = StageException.class)
  public void testInitLogDoesNotExist() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    String logFile = new File(testDataDir, "logFile.txt").getAbsolutePath();

    SourceRunner runner = new SourceRunner.Builder(FileTailSource.class)
        .addConfiguration("fileDataType", FileDataType.LOG_DATA)
        .addConfiguration("fileName", logFile)
        .addConfiguration("batchSize", 25)
        .addConfiguration("maxWaitTimeSecs", 1)
        .addOutputLane("lane")
        .build();
    runner.runInit();
  }

  @Test(expected = StageException.class)
  public void testInitLogNoPermissions() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    String logFile = new File(testDataDir, "logFile.txt").getAbsolutePath();

    File file = new File(logFile);
    Assert.assertTrue(file.createNewFile());
    try {
      Assert.assertTrue(file.setReadable(false));

      SourceRunner runner = new SourceRunner.Builder(FileTailSource.class)
          .addConfiguration("fileDataType", FileDataType.LOG_DATA)
          .addConfiguration("fileName", logFile)
          .addConfiguration("batchSize", 25)
          .addConfiguration("maxWaitTimeSecs", 1)
          .addOutputLane("lane")
          .build();
      runner.runInit();
    } finally {
      file.setReadable(true);
    }
  }

  @Test
  public void testTailLog() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    String logFile = new File(testDataDir, "logFile.txt").getAbsolutePath();
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("testLogFile.txt");
    OutputStream os = new FileOutputStream(logFile);
    IOUtils.copy(is, os);
    is.close();

    SourceRunner runner = new SourceRunner.Builder(FileTailSource.class)
        .addConfiguration("fileDataType", FileDataType.LOG_DATA)
        .addConfiguration("fileName", logFile)
        .addConfiguration("batchSize", 25)
        .addConfiguration("maxWaitTimeSecs", 1)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    Thread.sleep(500);
    os.write("HELLO\n".getBytes());
    Thread.sleep(500);
    try {
      long start = System.currentTimeMillis();
      StageRunner.Output output = runner.runProduce(null, 1000);
      long end = System.currentTimeMillis();
      Assert.assertTrue(end - start >= 1000);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(((FileTailSource) runner.getStage()).getFileOffset() + "::1", output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Record record = output.getRecords().get("lane").get(0);
      Assert.assertEquals("HELLO", record.get("/line").getValueAsString());
      Assert.assertEquals(((FileTailSource)runner.getStage()).getFileOffset() + "::0", record.getHeader().getSourceId());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTailJson() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    String logFile = new File(testDataDir, "logFile.txt").getAbsolutePath();
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("testLogFile.txt");
    OutputStream os = new FileOutputStream(logFile);
    IOUtils.copy(is, os);
    is.close();

    SourceRunner runner = new SourceRunner.Builder(FileTailSource.class)
        .addConfiguration("fileDataType", FileDataType.JSON_DATA)
        .addConfiguration("fileName", logFile)
        .addConfiguration("batchSize", 25)
        .addConfiguration("maxWaitTimeSecs", 1)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    Thread.sleep(500);
    os.write("{\"a\": 1}\n".getBytes());
    os.write("[{\"b\": 2}]\n".getBytes());
    Thread.sleep(500);
    try {
      long start = System.currentTimeMillis();
      StageRunner.Output output = runner.runProduce(null, 1000);
      long end = System.currentTimeMillis();
      Assert.assertTrue(end - start >= 1000);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(((FileTailSource) runner.getStage()).getFileOffset() + "::2", output.getNewOffset());
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Record record = output.getRecords().get("lane").get(0);
      Assert.assertEquals(1, record.get("/a").getValue());
      Assert.assertEquals(((FileTailSource)runner.getStage()).getFileOffset() + "::0", record.getHeader().getSourceId());
      record = output.getRecords().get("lane").get(1);
      Assert.assertEquals(2, record.get("[0]/b").getValue());
      Assert.assertEquals(((FileTailSource)runner.getStage()).getFileOffset() + "::1", record.getHeader().getSourceId());
    } finally {
      runner.runDestroy();
    }
  }

}
