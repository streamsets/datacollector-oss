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

import com.streamsets.datacollector.pipeline.executor.spark.SparkDExecutor;
import com.streamsets.datacollector.pipeline.executor.spark.SparkExecutor;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

// Due to a bug in PowerMock, we can't use it to run the test that run the actual executor,
// so we use PowerMockRunner only for the methods we need. All others simply use Mockito.
// Bug: https://github.com/powermock/powermock/issues/736
// This causes an exception when lambdas are called in YarnLauncher
@RunWith(PowerMockRunner.class)
@PrepareForTest({YarnAppLauncher.class})
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestSparkExecutorHomeDirectories extends BaseSparkExecutorTest {
  @Test(expected = StageException.class)
  public void testNoJavaHome() throws Exception {
    conf.javaHome = "";
    PowerMockito.mockStatic(System.class);
    Mockito.when(System.getenv("JAVA_HOME")).thenReturn("");

    SparkExecutor executor = new TestSparkExecutor.ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    Assert.fail();
  }

  @Test(expected = StageException.class)
  public void testNoSparkHome() throws Exception {

    conf.sparkHome = "";
    SparkExecutor executor = new TestSparkExecutor.ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    Assert.fail();
  }

  @Test
  public void testEnvJavaHome() throws Exception {
    String javaHome = testFolder.newFolder("java_home").toString();
    conf.javaHome = "";
    PowerMockito.mockStatic(System.class);
    Mockito.when(System.getenv("JAVA_HOME")).thenReturn(javaHome);

    SparkExecutor executor = new TestSparkExecutor.ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    runner.runDestroy();
  }

  @Test
  public void testEnvSparkHome() throws Exception {
    String sparkHome = testFolder.newFolder("spark_home").toString();
    conf.sparkHome = "";
    PowerMockito.mockStatic(System.class);
    Mockito.when(System.getenv("SPARK_HOME")).thenReturn(sparkHome);

    SparkExecutor executor = new TestSparkExecutor.ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    runner.runDestroy();
  }

  @Test(expected = StageException.class)
  public void testEnvJavaHomeInaccessible() throws Exception {
    conf.javaHome = "";
    PowerMockito.mockStatic(System.class);
    Mockito.when(System.getenv("JAVA_HOME")).thenReturn("/not/real");

    SparkExecutor executor = new TestSparkExecutor.ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    Assert.fail();
  }

  @Test(expected = StageException.class)
  public void testEnvSparkHomeInaccessible() throws Exception {
    conf.javaHome = "";
    PowerMockito.mockStatic(System.class);
    Mockito.when(System.getenv("SPARK_HOME")).thenReturn("/not/real");

    SparkExecutor executor = new TestSparkExecutor.ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    Assert.fail();
  }
}
