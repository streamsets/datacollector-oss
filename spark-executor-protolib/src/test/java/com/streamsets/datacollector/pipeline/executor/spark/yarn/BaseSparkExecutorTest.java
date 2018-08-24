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
package com.streamsets.datacollector.pipeline.executor.spark.yarn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.pipeline.executor.spark.AppLauncher;
import com.streamsets.datacollector.pipeline.executor.spark.DeployMode;
import com.streamsets.datacollector.pipeline.executor.spark.SparkExecutor;
import com.streamsets.datacollector.pipeline.executor.spark.SparkExecutorConfigBean;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class BaseSparkExecutorTest { //NOSONAR
  private final String JAVA_HOME = "java";
  private final String SPARK_HOME = "spark";
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  protected SparkExecutorConfigBean conf;
  protected SparkLauncher launcher;

  private String getTempFile(String fileName) throws IOException {
    return testFolder.newFile(fileName).toString();
  }

  @Before
  public void setUp() throws Exception {
    conf = new SparkExecutorConfigBean();
    conf.yarnConfigBean.appName = "test";
    conf.yarnConfigBean.deployMode = DeployMode.CLIENT;
    conf.javaHome = testFolder.newFolder(JAVA_HOME).toString();
    conf.sparkHome = testFolder.newFolder(SPARK_HOME).toString();
    conf.yarnConfigBean.appResource = getTempFile("resource");
    conf.yarnConfigBean.additionalJars = ImmutableList.of(getTempFile("jar1"), getTempFile("jar2"));
    conf.yarnConfigBean.additionalFiles = ImmutableList.of(getTempFile("file1"), getTempFile("file2"));
    conf.yarnConfigBean.appArgs = ImmutableList.of("frodo", "sam");
    conf.yarnConfigBean.waitForCompletion = false;
    conf.yarnConfigBean.dynamicAllocation = true;
    conf.yarnConfigBean.minExecutors = 1;
    conf.yarnConfigBean.maxExecutors = 4;
    conf.yarnConfigBean.driverMemory = "1g";
    conf.yarnConfigBean.executorMemory = "2g";
    conf.yarnConfigBean.mainClass = "org.mordor.mission.SecondDarkness";
    conf.yarnConfigBean.args = ImmutableMap.of("--tower1", "orthanc", "--tower2", "barad-dur");
    conf.yarnConfigBean.noValueArgs = ImmutableList.of("sauron", "saruman");
    conf.yarnConfigBean.env = ImmutableMap.of("king", "aragon", "status", "returned");
    conf.yarnConfigBean.language = Language.JVM;
  }

  private SparkLauncher getSparkLauncher() throws Exception {
    launcher = spy(new SparkLauncher(conf.yarnConfigBean.env));
    SparkAppHandle handle = mock(SparkAppHandle.class);
    doReturn("One Ring to Rule Them All").when(handle).getAppId();
    doReturn(handle).when(launcher).startApplication(any());
    return launcher;
  }

  private class FakeYarnLauncher extends YarnAppLauncher {

    @Override
    public SparkLauncher getLauncher() {
      try {
        return getSparkLauncher();
      } catch (Exception ex) {
        return null;
      }
    }
  }

  protected class ExtendedSparkExecutor extends SparkExecutor {

    ExtendedSparkExecutor(SparkExecutorConfigBean configs) {
      super(configs);
    }

    @Override
    public AppLauncher getLauncher() {
      return new FakeYarnLauncher();
    }
  }
}
