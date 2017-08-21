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
package com.streamsets.pipeline.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestHadoopMapReduceBinding {

  @Test
  public void testJavaOptsArgs() {
    Configuration conf = new Configuration();
    conf.set(HadoopMapReduceBinding.MAPREDUCE_MAP_MEMORY_MB, "1024");
    // Xmx with m option
    String javaOpts = "-Xmx1024m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug";
    assertEquals(Integer.valueOf(1280), HadoopMapReduceBinding.getMapMemoryMb(javaOpts, conf));
    // Xmx with g option
    javaOpts = "-Xmx1g -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug";
    assertEquals(Integer.valueOf(1280), HadoopMapReduceBinding.getMapMemoryMb(javaOpts, conf));
    // Xmx with k option
    javaOpts = "-Xmx1048576K -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug";
    assertEquals(Integer.valueOf(1280), HadoopMapReduceBinding.getMapMemoryMb(javaOpts, conf));
    // Xmx only bytes
    javaOpts = "-Xmx1073741824 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug";
    assertEquals(Integer.valueOf(1280), HadoopMapReduceBinding.getMapMemoryMb(javaOpts, conf));
    // last -Xmx wins
    javaOpts = "-Xmx512m -Xmx1024m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug";
    assertEquals(Integer.valueOf(1280), HadoopMapReduceBinding.getMapMemoryMb(javaOpts, conf));
    conf.set(HadoopMapReduceBinding.MAPREDUCE_MAP_MEMORY_MB, "2048");
    // default config > passed java_opts
    assertEquals(Integer.valueOf(2048), HadoopMapReduceBinding.getMapMemoryMb(javaOpts, conf));
    // no Xmx option
    javaOpts = "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug";
    assertNull(HadoopMapReduceBinding.getMapMemoryMb(javaOpts, conf));

  }
}
