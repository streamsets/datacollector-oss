/**
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.cluster;

import com.streamsets.datacollector.main.RuntimeInfo;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

public class TestClusterLogConfigUtils {

  @Rule
  public TemporaryFolder tempFolder= new TemporaryFolder();

  @Test
  public void testClusterLogContent() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    File configFolder = tempFolder.newFolder();
    Mockito.when(runtimeInfo.getConfigDir()).thenReturn(configFolder.getAbsolutePath());
    Mockito.when(runtimeInfo.getLog4jPropertiesFileName()).thenReturn("foo");
    try (PrintWriter printWriter = new PrintWriter(new File(runtimeInfo.getConfigDir(),
        runtimeInfo.getLog4jPropertiesFileName()))) {
      printWriter.println("log4j.rootLogger=INFO, streamsets");
      printWriter.println("log4j.appender.stdout.Target=System.out");
      printWriter.println("log4j.logger.com.streamsets.pipeline=INFO");

    }
    List<String> lines = ClusterLogConfigUtils.getLogContent(runtimeInfo, "/cluster-spark-log4j.properties");
    Assert.assertEquals("log4j.logger.com.streamsets.pipeline=INFO", lines.get(lines.size() - 1));

    lines = ClusterLogConfigUtils.getLogContent(runtimeInfo, "/cluster-mr-log4j.properties");
    Assert.assertEquals("log4j.logger.com.streamsets.pipeline=INFO", lines.get(lines.size() - 1));
  }

}
