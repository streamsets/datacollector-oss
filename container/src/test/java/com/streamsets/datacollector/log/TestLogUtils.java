/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.log;

import com.streamsets.datacollector.log.LogUtils;
import com.streamsets.datacollector.main.RuntimeInfo;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.UUID;

public class TestLogUtils {

  @Test
  public void testResolveValue() {
    String sysProp = System.getProperty("user.home");
    Assert.assertEquals("a", LogUtils.resolveValue("a"));
    Assert.assertEquals(sysProp, LogUtils.resolveValue("${user.home}"));
    Assert.assertEquals("a" + sysProp + "b" + sysProp, LogUtils.resolveValue("a${user.home}b${user.home}"));
  }

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
    writer.write(LogUtils.LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY + "=" + logFile.getAbsolutePath());
    writer.close();
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getAttribute(Mockito.eq(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR)))
           .thenReturn(log4fConfig.toURI().toURL());
    Assert.assertEquals(logFile.getAbsolutePath(), LogUtils.getLogFile(runtimeInfo));
  }

}
