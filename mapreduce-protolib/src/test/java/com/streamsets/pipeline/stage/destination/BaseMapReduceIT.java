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
package com.streamsets.pipeline.stage.destination;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.config.JobConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.config.JobType;
import com.streamsets.pipeline.stage.destination.mapreduce.config.MapReduceConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.SimpleJobCreator;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert.AvroConversionCommonConfig;
import com.streamsets.pipeline.lib.converter.AvroParquetConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Map;

abstract public class BaseMapReduceIT {

  @Rule public TestName testName = new TestName();

  private static String testCaseBaseDir = "target/test-data/";
  private String baseDir;
  private String confDir;
  private String inputDir;
  private String outputDir;

  @Before
  public void setUp() throws Exception {
    baseDir = testCaseBaseDir + "/" + getClass().getCanonicalName() + "/" + testName.getMethodName() + "/";
    FileUtils.deleteQuietly(new File(baseDir));

    confDir = baseDir + "conf/";
    inputDir = baseDir + "input/";
    outputDir = baseDir + "/output/";

    new File(confDir).mkdirs();
    new File(inputDir).mkdirs();

    // We're using LocalJobRunner for the tests, so we just create default files on disk to pass validations
    for(String configFile : ImmutableList.of("core-site.xml", "yarn-site.xml", "mapred-site.xml", "hdfs-site.xml")) {
      writeConfiguration(new Configuration(), new File(confDir, configFile));
    }
  }

  public MapReduceExecutor generateExecutor(
      AvroConversionCommonConfig commonConfig,
      AvroParquetConfig avroParquetConfig,
      Map<String, String> jobConfigs
  ) {
    return generateExecutor(commonConfig, avroParquetConfig, JobType.AVRO_PARQUET, jobConfigs);
  }

  public MapReduceExecutor generateExecutor(Map<String, String> jobConfigs) {
    return generateExecutor(null, null, JobType.CUSTOM, jobConfigs);
  }

  public MapReduceExecutor generateExecutor(
      AvroConversionCommonConfig commonConfig,
      AvroParquetConfig avroParquetConfig,
      JobType jobType,
      Map<String, String> jobConfigs
  ) {
    MapReduceConfig mapReduceConfig = new MapReduceConfig();
    mapReduceConfig.mapReduceConfDir = getConfDir();
    mapReduceConfig.mapreduceConfigs = Collections.emptyMap();
    mapReduceConfig.mapreduceUser = "";
    mapReduceConfig.kerberos = false;

    JobConfig jobConfig = new JobConfig();
    jobConfig.jobType = jobType;
    jobConfig.jobConfigs = jobConfigs;
    jobConfig.customJobCreator = SimpleJobCreator.class.getCanonicalName();
    jobConfig.jobName = "SDC Test Job";
    jobConfig.avroConversionCommonConfig = commonConfig;
    jobConfig.avroParquetConfig = avroParquetConfig;

    MapReduceExecutor executor = new MapReduceExecutor(mapReduceConfig, jobConfig);
    executor.waitForCompletition = true;
    return executor;
  }

  private static void writeConfiguration(Configuration conf, File outputFile) throws Exception {
    FileOutputStream outputStream = new FileOutputStream(outputFile);
    conf.writeXml(outputStream);
    outputStream.close();
  }

  public String getConfDir() {
    return confDir;
  }

  public String getInputDir() {
    return inputDir;
  }

  public String getOutputDir() {
    return outputDir;
  }
}
