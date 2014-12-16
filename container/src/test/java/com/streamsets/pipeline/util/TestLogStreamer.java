/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.main.RuntimeInfo;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.UUID;

public class TestLogStreamer {

  @Test
  public void testResolveValue() {
    String sysProp = System.getProperty("user.home");
    Assert.assertEquals("a", LogStreamer.resolveValue("a"));
    Assert.assertEquals(sysProp, LogStreamer.resolveValue("${user.home}"));
    Assert.assertEquals("a" + sysProp + "b" + sysProp, LogStreamer.resolveValue("a${user.home}b${user.home}"));
  }

  @Test
  public void testNoLogFileInRuntimeInfo() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    LogStreamer logStreamer = new LogStreamer(runtimeInfo, null);
    Assert.assertNull(logStreamer.getLogFile());
    List<String> lines = IOUtils.readLines(logStreamer.getLogTailReader());
    Assert.assertEquals(1, lines.size());
    Assert.assertEquals(LogStreamer.LOG_NOT_AVAILABLE_LINE, lines.get(0));
  }

  private boolean release;

  @Test
  public void testLogFileInRuntimeInfo() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    final File logFile = new File(dir, "test.log");
    Writer writer = new FileWriter(logFile);
    writer.write("hello\n");
    writer.close();
    File log4fConfig = new File(dir, "log4j.properties");
    writer = new FileWriter(log4fConfig);
    writer.write(LogStreamer.LOG4J_APPENDER_STREAMSETS_FILE + "=" + logFile.getAbsolutePath());
    writer.close();
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getAttribute(Mockito.eq(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR)))
           .thenReturn(log4fConfig.toURI().toURL());
    LogStreamer.Releaser releaser = new LogStreamer.Releaser() {
      @Override
      public void release() {
        release = true;
      }
    };
    release = false;
    LogStreamer logStreamer = new LogStreamer(runtimeInfo, releaser);
    Assert.assertEquals(logFile.getAbsolutePath(), logStreamer.getLogFile());
    BufferedReader reader = new BufferedReader(logStreamer.getLogTailReader());
    new Thread() {
      @Override
      public void run() {
        try {
          Writer writer = new FileWriter(logFile, true);
          writer.write("hello\n");
          writer.close();
        } catch (IOException ex) {
          Assert.fail();
        }
      }
    }.start();
    Assert.assertEquals("hello", reader.readLine());
    reader.close();
    Assert.assertTrue(release);
  }

}
