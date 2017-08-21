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
package com.streamsets.datacollector.main;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.helpers.FileWatchdog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.streamsets.datacollector.main.LogConfigurator;
import com.streamsets.datacollector.main.RuntimeInfo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.UUID;

public class TestLogConfigurator {

  @Before
  public void setUp() throws Exception {
    cleanUp();
  }

  @After
  public void cleanUp() throws Exception {
    System.getProperties().remove("log4j.configuration");
    System.getProperties().remove("log4j.defaultInitOverride");
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      if (thread instanceof FileWatchdog) {
        Field interrupted = ((Class)thread.getClass().getGenericSuperclass()).getDeclaredField("interrupted");
        interrupted.setAccessible(true);
        interrupted.set(thread, true);
        thread.interrupt();
      }
    }
  }

  @Test
  public void testExternalLog4jConfig() {
    System.setProperty("log4j.configuration", "foo");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    new LogConfigurator(runtimeInfo).configure();
    Mockito.verify(runtimeInfo, Mockito.times(0)).getConfigDir();
    Mockito.verify(runtimeInfo, Mockito.times(0)).setAttribute(Mockito.eq(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR),
                                                               Mockito.any());
  }

  @Test
  public void testClasspathLog4jConfig() {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    File configDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(configDir.mkdirs());
    Mockito.when(runtimeInfo.getConfigDir()).thenReturn(configDir.getAbsolutePath());
    Mockito.when(runtimeInfo.getLog4jPropertiesFileName()).thenReturn("log4j.properties");
    new LogConfigurator(runtimeInfo).configure();
    Mockito.verify(runtimeInfo, Mockito.times(1)).getConfigDir();
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
     Assert.assertFalse(thread instanceof FileWatchdog);
    }
    Mockito.verify(runtimeInfo, Mockito.times(1)).setAttribute(Mockito.eq(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR),
                                                               Mockito.any());
  }

  @Test
  public void testConfigDirLog4jConfig() throws IOException {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    File configDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(configDir.mkdirs());
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("log4j.properties");
    OutputStream os = new FileOutputStream(new File(configDir, "log4j.properties"));
    IOUtils.copy(is, os);
    is.close();
    os.close();
    Mockito.when(runtimeInfo.getConfigDir()).thenReturn(configDir.getAbsolutePath());
    Mockito.when(runtimeInfo.getLog4jPropertiesFileName()).thenReturn("log4j.properties");
    new LogConfigurator(runtimeInfo).configure();
    Mockito.verify(runtimeInfo, Mockito.times(1)).getConfigDir();
    boolean foundFileWatcher = false;
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      foundFileWatcher |= (thread instanceof FileWatchdog);
    }
    Assert.assertTrue(foundFileWatcher);
    Mockito.verify(runtimeInfo, Mockito.times(1)).setAttribute(Mockito.eq(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR),
                                                               Mockito.any());
  }
}
