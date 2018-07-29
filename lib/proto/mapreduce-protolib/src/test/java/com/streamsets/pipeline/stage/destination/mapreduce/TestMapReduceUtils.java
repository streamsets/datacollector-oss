/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class TestMapReduceUtils {

  private static final String HIVE_SERDE_JAR = "file:/my/dir/hive-serde-1.2-1.1.0-cdh5.12.2.jar";
  private static final String HIVE_EXEC_JAR = "file:/my/dir/hive-exec-1.2-1.1.0-cdh5.12.2.jar";
  private static final String AVRO_MAPRED_JAR = "file:/my/dir/avro-mapred-1.7.6-cdh5.12.2.jar";

  private static final URL[] URLS = new URL[3];

  @BeforeClass
  public static void initUrls() {
    try {
      URLS[0] = new URL(HIVE_SERDE_JAR);
      URLS[1] = new URL(HIVE_EXEC_JAR);
      URLS[2] = new URL(AVRO_MAPRED_JAR);
    } catch (MalformedURLException e) {
      throw new IllegalStateException("Exception initializing urls", e);
    }
  }

  @Test
  public void simple() throws MalformedURLException {
    final Configuration conf = new Configuration();

    MapreduceUtils.addJarsToJob(conf, false, URLS, "hive-exec");
    assertThat(conf.get(MapreduceUtils.TMP_JARS_PROPERTY), equalTo(HIVE_EXEC_JAR));
  }

  @Test
  public void multipleOneCall() throws MalformedURLException {
    final Configuration conf = new Configuration();

    MapreduceUtils.addJarsToJob(conf, false, URLS, "hive-serde", "avro-mapred");
    final String expected = HIVE_SERDE_JAR + MapreduceUtils.JAR_SEPARATOR + AVRO_MAPRED_JAR;
    assertThat(conf.get(MapreduceUtils.TMP_JARS_PROPERTY), equalTo(expected));
  }

  @Test
  public void multipleCalls() throws MalformedURLException {
    final Configuration conf = new Configuration();

    MapreduceUtils.addJarsToJob(conf, false, URLS, "avro-mapred");
    assertThat(conf.get(MapreduceUtils.TMP_JARS_PROPERTY), equalTo(AVRO_MAPRED_JAR));
    MapreduceUtils.addJarsToJob(conf, false, URLS, "hive-serde");
    final String expected = AVRO_MAPRED_JAR + MapreduceUtils.JAR_SEPARATOR + HIVE_SERDE_JAR;
    assertThat(conf.get(MapreduceUtils.TMP_JARS_PROPERTY), equalTo(expected));
  }

  @Test(expected = IllegalArgumentException.class)
  public void notFound() throws MalformedURLException {
    final Configuration conf = new Configuration();

    MapreduceUtils.addJarsToJob(conf, false, URLS, "hive-serde", "orc-core");
  }

  @Test(expected = IllegalArgumentException.class)
  public void multipleMatchesNotAllowed() throws MalformedURLException {
    final Configuration conf = new Configuration();

    MapreduceUtils.addJarsToJob(conf, false, URLS, "hive");
  }

  @Test
  public void multipleMatchesAllowed() throws MalformedURLException {
    final Configuration conf = new Configuration();

    MapreduceUtils.addJarsToJob(conf, true, URLS, "hive");
    final String expected = HIVE_SERDE_JAR + MapreduceUtils.JAR_SEPARATOR + HIVE_EXEC_JAR;
    assertThat(conf.get(MapreduceUtils.TMP_JARS_PROPERTY), equalTo(expected));
  }
}
