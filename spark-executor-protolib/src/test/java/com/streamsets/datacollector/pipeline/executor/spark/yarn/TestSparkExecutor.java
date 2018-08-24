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
import com.streamsets.datacollector.pipeline.executor.spark.SparkDExecutor;
import com.streamsets.datacollector.pipeline.executor.spark.SparkExecutor;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestSparkExecutor extends BaseSparkExecutorTest {

  private void verifyMethodCalls() throws IOException {
    verify(launcher, times(1))
        .setMaster(eq("yarn"));
    verify(launcher, times(1))
        .setDeployMode(eq(conf.yarnConfigBean.deployMode.getLabel().toLowerCase()));
    verify(launcher, times(1)).setAppName(eq(conf.yarnConfigBean.appName));
    verify(launcher, times(1)).setSparkHome(eq(conf.sparkHome));
    verify(launcher, times(1)).setJavaHome(eq(conf.javaHome));
    verify(launcher, times(1)).setAppResource(eq(conf.yarnConfigBean.appResource));

    conf.yarnConfigBean.additionalJars.forEach((String jar) ->
        verify(launcher, times(1)).addJar(eq(jar)));
    conf.yarnConfigBean.additionalFiles.forEach((String file) ->
        verify(launcher, times(1)).addFile(eq(file)));

    verify(launcher, times(1)).setMainClass(eq(conf.yarnConfigBean.mainClass));

    if (conf.yarnConfigBean.dynamicAllocation) {
      verify(launcher, times(1)).setConf("spark.dynamicAllocation.enabled", "true");
      verify(launcher, times(1)).setConf("spark.shuffle.service.enabled", "true");
      verify(launcher, times(1)).
          setConf(eq("spark.dynamicAllocation.minExecutors"), eq(String.valueOf(conf.yarnConfigBean.minExecutors)));
      verify(launcher, times(1)).
          setConf(eq("spark.dynamicAllocation.maxExecutors"), eq(String.valueOf(conf.yarnConfigBean.maxExecutors)));
      verify(launcher, never()).
          addSparkArg(eq("--num-executors"), eq(String.valueOf(conf.yarnConfigBean.numExecutors)));

    } else {
      verify(launcher, times(1)).
          addSparkArg(eq("--num-executors"), eq(String.valueOf(conf.yarnConfigBean.numExecutors)));
      verify(launcher, never()).setConf("spark.dynamicAllocation.enabled", "true");
      verify(launcher, never()).setConf("spark.shuffle.service.enabled", "true");
      verify(launcher, never()).
          setConf(eq("spark.dynamicAllocation.minExecutors"), eq(String.valueOf(conf.yarnConfigBean.minExecutors)));
      verify(launcher, never()).
          setConf(eq("spark.dynamicAllocation.maxExecutors"), eq(String.valueOf(conf.yarnConfigBean.maxExecutors)));
    }

    verify(launcher, times(1))
        .addSparkArg(eq("--executor-memory"), eq(conf.yarnConfigBean.executorMemory));
    verify(launcher, times(1))
        .addSparkArg(eq("--driver-memory"), eq(conf.yarnConfigBean.driverMemory));

    conf.yarnConfigBean.args.forEach((String key, String val) ->
        verify(launcher, times(1)).addSparkArg(key, val));


    conf.yarnConfigBean.noValueArgs.forEach((String val) ->
        verify(launcher, times(1)).addSparkArg(val));

    verify(launcher, times(1)).startApplication(any());

  }

  private void verifyAppArgs() {
    verify(launcher, times(1))
        .addAppArgs(conf.yarnConfigBean.appArgs.toArray(new String[conf.yarnConfigBean.appArgs.size()]));

  }

  @Test
  public void testBasic() throws Exception {

    SparkExecutor executor = new ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(RecordCreator.create()));
    verifyMethodCalls();
    verifyAppArgs();
    runner.runDestroy();
  }


  @Test
  public void testBasicNoDynamicAllocation() throws Exception {

    conf.yarnConfigBean.dynamicAllocation = false;
    conf.yarnConfigBean.numExecutors = 100;
    SparkExecutor executor = new ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(RecordCreator.create()));
    verifyMethodCalls();
    verifyAppArgs();
    runner.runDestroy();
  }

  @Test
  public void testEventGen() throws Exception {

    conf.yarnConfigBean.dynamicAllocation = false;
    conf.yarnConfigBean.numExecutors = 100;
    SparkExecutor executor = new ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(RecordCreator.create()));
    verifyMethodCalls();
    verifyAppArgs();
    List<EventRecord> events = runner.getEventRecords();
    runner.runDestroy();
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(events.get(0).get("/app-id").getValueAsString(), "One Ring to Rule Them All");
  }

  @Test
  public void testEL() throws Exception {

    conf.yarnConfigBean.appArgs = ImmutableList.of("${record:value(\"/king\")}", "${record:value(\"/precious\")}");

    SparkExecutor executor = new ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    Record record = RecordCreator.create();
    Map<String, Field> root = new HashMap<>();
    root.put("king", Field.create("Aragon"));
    root.put("precious", Field.create("Gollum"));
    record.set(Field.create(root));
    runner.runWrite(ImmutableList.of(record));
    verifyMethodCalls();
    verify(launcher, times(1)).addAppArgs("Aragon", "Gollum");

    runner.runDestroy();
  }

  @Test
  public void testEmptyEL() throws Exception {

    conf.yarnConfigBean.appArgs = ImmutableList.of("${record:value(\"/king\")}", "${record:value(\"/ringbearer\")}");

    SparkExecutor executor = new ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    Record record = RecordCreator.create();
    Map<String, Field> root = new HashMap<>();
    root.put("king", Field.create("Aragon"));
    root.put("precious", Field.create("Gollum"));
    record.set(Field.create(root));
    runner.runWrite(ImmutableList.of(record));
    verifyMethodCalls();
    verify(launcher, times(1)).addAppArgs("Aragon");

    runner.runDestroy();
  }

  @Test(expected = StageException.class)
  public void testBadMemoryString() throws Exception {

    conf.yarnConfigBean.executorMemory = "40404ggh";
    SparkExecutor executor = new ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
  }

  @Test(expected = StageException.class)
  public void testMissingValues() throws Exception {
    conf.yarnConfigBean.env = ImmutableMap.of("--test", "");
    doTestBadKeyValues();
  }

  @Test(expected = StageException.class)
  public void testMissingKeys() throws Exception {
    conf.yarnConfigBean.env = ImmutableMap.of("", "missingkey");
    doTestBadKeyValues();
  }

  @Test(expected = StageException.class)
  public void testMissingValuesArgs() throws Exception {
    conf.yarnConfigBean.args = ImmutableMap.of("--test", "");
    doTestBadKeyValues();
  }

  @Test(expected = StageException.class)
  public void testMissingKeysArgs() throws Exception {
    conf.yarnConfigBean.args = ImmutableMap.of("", "missingkey");
    doTestBadKeyValues();
  }

  private void doTestBadKeyValues() throws Exception {

    SparkExecutor executor = new ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
  }

  @Test(expected = StageException.class)
  public void testVerifyInaccessibleJavaHome() throws Exception {
    conf.javaHome = "/non/existent/file1";
    doTestNonExistentFile();
  }

  @Test(expected = StageException.class)
  public void testVerifyInaccessibleSparkHome() throws Exception {
    conf.sparkHome = "/non/existent/file2";
    doTestNonExistentFile();
  }

  @Test(expected = StageException.class)
  public void testVerifyInaccessibleAppResource() throws Exception {
    conf.yarnConfigBean.appResource = "/non/existent/resource";
    doTestNonExistentFile();
  }

  @Test(expected = StageException.class)
  public void testVerifyInaccessibleAdditionalJar() throws Exception {
    conf.yarnConfigBean.additionalJars = ImmutableList.of("/non/existent/additionalJar");
    doTestNonExistentFile();
  }

  @Test(expected = StageException.class)
  public void testVerifyInaccessibleAdditionalFile() throws Exception {
    conf.yarnConfigBean.additionalFiles = ImmutableList.of("/non/existent/additionalJar");
    doTestNonExistentFile();
  }

  private void doTestNonExistentFile() throws Exception {
    SparkExecutor executor = new ExtendedSparkExecutor(conf);

    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    Assert.fail();
  }

}
