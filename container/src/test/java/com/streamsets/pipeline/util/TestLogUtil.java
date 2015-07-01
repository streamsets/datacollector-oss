/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.log4j.Logger;
import org.junit.Test;


public class TestLogUtil {

  @Test
  public void testRegisterLogger() {
    assertTrue(LogUtil.registerLogger("aaa", "1", "0", new File("target/dummy").getAbsolutePath(), new Configuration()));
    assertFalse(LogUtil.registerLogger("aaa", "1", "0", new File("target/dummy").getAbsolutePath(), new Configuration()));
  }

  @Test
  public void testAppender() {
    LogUtil.registerLogger("aaa", "1", "0", new File("target/dummy").getAbsolutePath(), new Configuration());
    String loggerName = LogUtil.getLoggerName("aaa", "1", "0");
    Logger logger = Logger.getLogger(loggerName);
    assertNotNull(logger.getAppender(loggerName));
    LogUtil.resetRollingFileAppender("aaa", "1", "0");
    assertNull(logger.getAppender(loggerName));
    assertTrue(LogUtil.registerLogger("aaa", "1", "0", new File("target/dummy").getAbsolutePath(), new Configuration()));
  }

}
