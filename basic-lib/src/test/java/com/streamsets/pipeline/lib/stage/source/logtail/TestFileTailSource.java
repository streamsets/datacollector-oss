/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.logtail;

import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

public class TestFileTailSource {
  private String logFile;

  @Test
  public void testTail() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    String logFile = new File(testDataDir, "logFile.txt").getAbsolutePath();
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("testLogFile.txt");
    OutputStream os = new FileOutputStream(logFile);
    IOUtils.copy(is, os);
    is.close();

    SourceRunner runner = new SourceRunner.Builder(FileTailSource.class)
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
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Assert.assertEquals("HELLO", output.getRecords().get("lane").get(0).get("/line").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

}
