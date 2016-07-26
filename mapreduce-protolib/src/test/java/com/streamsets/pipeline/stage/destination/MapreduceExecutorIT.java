/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceDExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.config.JobConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.config.JobType;
import com.streamsets.pipeline.stage.destination.mapreduce.config.MapReduceConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.SimpleJobCreator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.UUID;

public class MapreduceExecutorIT {
  @Rule public TestName testName = new TestName();

  private static String baseDir = "target/test-data/mapreduce-executor-" + UUID.randomUUID().toString();
  private static String confDir = baseDir + "/conf/";
  private String inputDir;
  private String outputDir;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // Recreate working directories
    FileUtils.deleteQuietly(new File(baseDir));
    new File(confDir).mkdirs();

    // We're using LocalJobRunner for the tests, so we just create default files on disk to pass validations
    for(String configFile : ImmutableList.of("core-site.xml", "yarn-site.xml", "mapred-site.xml")) {
      writeConfiguration(new Configuration(), new File(confDir, configFile));
    }
  }

  @Before
  public void setUp() throws Exception {
    inputDir = baseDir + "/" + testName + "/input/";
    outputDir = baseDir + "/" + testName + "/output/";

    new File(inputDir).mkdirs();
  }


  @Test
  public void testSimpleJobExecutionThatFails() throws Exception{
    MapReduceExecutor executor = generateExecutor(ImmutableMap.<String, String>builder()
      .put(SimpleTestInputFormat.THROW_EXCEPTION, "true")
    .build());

    TargetRunner runner = new TargetRunner.Builder(MapReduceDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(ImmutableMap.of(
      "key", Field.create("value")
    )));

    runner.runWrite(ImmutableList.of(record));

    Assert.assertEquals(1, runner.getErrorRecords().size());

    runner.runDestroy();
  }

  @Test
  public void testSimpleJobExecution() throws Exception{
    File validationFile = new File(inputDir, "created");
    MapReduceExecutor executor = generateExecutor(ImmutableMap.<String, String>builder()
      .put(SimpleTestInputFormat.FILE_LOCATION, validationFile.getAbsolutePath())
      .put(SimpleTestInputFormat.FILE_VALUE, "secret")
    .build());

    TargetRunner runner = new TargetRunner.Builder(MapReduceDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(ImmutableMap.of(
      "key", Field.create("value")
    )));

    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals(0, runner.getErrorRecords().size());

    // Event should be properly generated
    Assert.assertEquals(1, runner.getEventRecords().size());
    Record event = runner.getEventRecords().get(0);
    Assert.assertNotNull(event.get("/job-id").getValueAsString());

    // And we should create a special mark file while starting the mr job
    Assert.assertTrue(validationFile.exists());
    Assert.assertEquals("secret", FileUtils.readFileToString(validationFile));

    runner.runDestroy();
  }

  @Test
  public void testSimpleJobExecutionEvaluateExpression() throws Exception{
    File validationFile = new File(inputDir, "created");
    MapReduceExecutor executor = generateExecutor(ImmutableMap.<String, String>builder()
      .put(SimpleTestInputFormat.FILE_LOCATION, validationFile.getAbsolutePath())
      .put(SimpleTestInputFormat.FILE_VALUE, "${record:value('/key')}")
    .build());

    TargetRunner runner = new TargetRunner.Builder(MapReduceDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(ImmutableMap.of(
      "key", Field.create("value")
    )));

    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals(0, runner.getErrorRecords().size());

    // Event should be properly generated
    Assert.assertEquals(1, runner.getEventRecords().size());
    Record event = runner.getEventRecords().get(0);
    Assert.assertNotNull(event.get("/job-id").getValueAsString());

    // And we should create a special mark file while starting the mr job
    Assert.assertTrue(validationFile.exists());
    Assert.assertEquals("value", FileUtils.readFileToString(validationFile));

    runner.runDestroy();
  }

  private MapReduceExecutor generateExecutor(Map<String, String> jobConfigs) {
    MapReduceConfig mapReduceConfig = new MapReduceConfig();
    mapReduceConfig.mapReduceConfDir = confDir;
    mapReduceConfig.mapreduceConfigs = ImmutableMap.<String, String>builder()
      .put("mapreduce.job.inputformat.class", SimpleTestInputFormat.class.getCanonicalName())
      .put("mapreduce.output.fileoutputformat.outputdir", outputDir)
      .build();
    mapReduceConfig.mapreduceUser = "";
    mapReduceConfig.kerberos = false;

    JobConfig jobConfig = new JobConfig();
    jobConfig.jobType = JobType.CUSTOM;
    jobConfig.customJobCreator = SimpleJobCreator.class.getCanonicalName();
    jobConfig.jobConfigs = jobConfigs;
    jobConfig.jobName = "SDC Test Job";

    return new MapReduceExecutor(mapReduceConfig, jobConfig);
  }

  private static void writeConfiguration(Configuration conf, File outputFile) throws Exception {
    FileOutputStream outputStream = new FileOutputStream(outputFile);
    conf.writeXml(outputStream);
    outputStream.close();
  }

}
