/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.LogUtil;


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
