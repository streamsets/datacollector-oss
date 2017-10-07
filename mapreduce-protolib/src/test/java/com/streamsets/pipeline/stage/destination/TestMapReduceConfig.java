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

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceErrors;
import com.streamsets.pipeline.stage.destination.mapreduce.config.MapReduceConfig;
import jersey.repackaged.com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestMapReduceConfig {
  private static String confDir = new File("target/test-data/mapreduce-config-" + UUID.randomUUID().toString()).getAbsolutePath();
  private MapReduceConfig config;
  private Stage.Context context;

  @BeforeClass
  public static void setUpClass() {
    new File(confDir).mkdirs();
  }

  @Before
  public void setUp() {
    config = new MapReduceConfig();
    config.mapReduceConfDir = confDir;
    config.mapreduceConfigs = Collections.emptyMap();
    config.mapreduceUser = "doctor-who";
    config.kerberos = false;

    context = mock(Stage.Context.class);

    FileUtils.deleteQuietly(new File(confDir, "yarn-site.xml"));
    FileUtils.deleteQuietly(new File(confDir, "core-site.xml"));
    FileUtils.deleteQuietly(new File(confDir, "mapred-site.xml"));
    FileUtils.deleteQuietly(new File(confDir, "hdfs-site.xml"));
  }

  @Test
  public void testInitWithIncorrectConfDir() {
    config.mapReduceConfDir = "/universe/andromeda-galaxy/another-earth/hodaap/conf/";
    List<Stage.ConfigIssue> issues = config.init(context, "prefix");
    Assert.assertEquals(1, issues.size());
    verify(context).createConfigIssue(anyString(), anyString(), eq(MapReduceErrors.MAPREDUCE_0003), anyString());
  }

  @Test
  public void testInitWithMissingConfFiles() {
    List<Stage.ConfigIssue> issues = config.init(context, "prefix");
    Assert.assertEquals(4, issues.size());
    verify(context).createConfigIssue(anyString(), anyString(), eq(MapReduceErrors.MAPREDUCE_0002), contains("core-site.xml"));
    verify(context).createConfigIssue(anyString(), anyString(), eq(MapReduceErrors.MAPREDUCE_0002), contains("yarn-site.xml"));
    verify(context).createConfigIssue(anyString(), anyString(), eq(MapReduceErrors.MAPREDUCE_0002), contains("mapred-site.xml"));
    verify(context).createConfigIssue(anyString(), anyString(), eq(MapReduceErrors.MAPREDUCE_0002), contains("hdfs-site.xml"));
  }

  @Test
  public void testInitAllSources() throws Exception {
    Configuration coreConf = new Configuration();
    coreConf.set("coreConf", "present");
    writeConfiguration(coreConf, new File(confDir, "core-site.xml"));

    Configuration yarnConf = new Configuration();
    yarnConf.set("yarnConf", "present");
    writeConfiguration(yarnConf, new File(confDir, "yarn-site.xml"));

    Configuration mapreduceConf = new Configuration();
    mapreduceConf.set("mapreduceConf", "present");
    writeConfiguration(mapreduceConf, new File(confDir, "mapred-site.xml"));

    Configuration hdfsConf = new Configuration();
    hdfsConf.set("hdfsConf", "present");
    writeConfiguration(hdfsConf, new File(confDir, "hdfs-site.xml"));

    config.mapreduceConfigs = ImmutableMap.of("sdcConf", "present");

    List<Stage.ConfigIssue> issues = config.init(context, "prefix");
    Assert.assertEquals(0, issues.size());

    Configuration finalConf = config.getConfiguration();
    for(String property : ImmutableList.of("coreConf", "yarnConf", "mapreduceConf", "sdcConf", "hdfsConf")) {
      Assert.assertEquals("Key is not present: " + property, "present", finalConf.get(property));
    }
  }

  private static void writeConfiguration(Configuration conf, File outputFile) throws Exception {
    FileOutputStream outputStream = new FileOutputStream(outputFile);
    conf.writeXml(outputStream);
    outputStream.close();
  }

}
