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

import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.pipeline.ClusterBinding;
import com.streamsets.pipeline.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HadoopMapReduceBinding implements ClusterBinding {
  private final String[] args;
  private Properties properties;
  private Job job;
  // JVM heap for map task
  public static final String MAPREDUCE_JAVA_OPTS = "mapreduce.map.java.opts";
  // Total physical memory in MB for a map task
  public static final String MAPREDUCE_MAP_MEMORY_MB = "mapreduce.map.memory.mb";

  public HadoopMapReduceBinding(String[] args) {
    this.args = args;
  }

  @Override
  public void init() throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = parser.getRemainingArgs();
    properties = new Properties();
    if (remainingArgs.length != 2) {
      List<String> argsList = new ArrayList<>();
      for (String arg : remainingArgs) {
        argsList.add("'" + arg + "'");
      }
      throw new IllegalArgumentException("Error expected properties-file java-opts got: " + argsList);
    }
    String propertiesFile = remainingArgs[0];
    String javaOpts = remainingArgs[1];
    try (InputStream in = new FileInputStream(propertiesFile)) {
      properties.load(in);
      String dataFormat = Utils.getHdfsDataFormat(properties);
      String source = this.getClass().getSimpleName();
      for (Object key : properties.keySet()) {
        String realKey = String.valueOf(key);
        String value = Utils.getPropertyNotNull(properties, realKey);
        conf.set(realKey, value, source);
      }
      Integer mapMemoryMb = getMapMemoryMb(javaOpts, conf);
      if (mapMemoryMb != null) {
        conf.set(MAPREDUCE_MAP_MEMORY_MB, String.valueOf(mapMemoryMb));
      }
      conf.set(MAPREDUCE_JAVA_OPTS, javaOpts);
      conf.setBoolean("mapreduce.map.speculative", false);
      conf.setBoolean("mapreduce.reduce.speculative", false);
      if ("AVRO".equalsIgnoreCase(dataFormat)) {
        conf.set(Job.INPUT_FORMAT_CLASS_ATTR, "org.apache.avro.mapreduce.AvroKeyInputFormat");
        conf.set(Job.MAP_OUTPUT_KEY_CLASS, "org.apache.avro.mapred.AvroKey");
      }
      job = Job.getInstance(conf, "StreamSets Data Collector: " + properties.getProperty(ClusterModeConstants.CLUSTER_PIPELINE_TITLE));
      job.setJarByClass(this.getClass());
      job.setNumReduceTasks(0);
      if (!"AVRO".equalsIgnoreCase(dataFormat)) {
        job.setOutputKeyClass(NullWritable.class);
      }
      job.setMapperClass(PipelineMapper.class);

      job.setOutputValueClass(NullWritable.class);

      job.setOutputFormatClass(NullOutputFormat.class);
    }
  }

  // visible for testing (can't annotate as can't depend on Guava)
  public static Integer getMapMemoryMb(String javaOpts, Configuration conf) {
    String[] javaOptsArray = javaOpts.split(" ");
    Integer upperLimitMemory = null;
    for (String opts : javaOptsArray) {
      if (opts.contains("-Xmx")) {
        Integer memoryMb = Integer.valueOf(opts.substring(4, opts.length() - 1));
        switch (opts.charAt(opts.length() - 1)) {
          case 'm':
          case 'M':
            break;
          case 'k':
          case 'K':
            memoryMb = memoryMb / (1024);
            break;
          case 'g':
          case 'G':
            memoryMb = memoryMb * 1024;
            break;
          default:
            memoryMb = Integer.valueOf(opts.substring(4, opts.length())) / (1024 * 1024);
            break;
        }
        // Add 25% to Java heap as MAP_MEMORY_MB is the total physical memory for the map task
        upperLimitMemory = ((int) (memoryMb * 0.25)) + memoryMb;
        // dont break as there could be multiple -Xmx, we need to honor the last
      }
    }
    if (upperLimitMemory != null) {
      String defaultMapMemoryString = conf.get(MAPREDUCE_MAP_MEMORY_MB);
      if (defaultMapMemoryString != null) {
        Integer defaultMapMemory = Integer.valueOf(defaultMapMemoryString);
        upperLimitMemory = (upperLimitMemory > defaultMapMemory ? upperLimitMemory : defaultMapMemory);
      }
    }
    return upperLimitMemory;
  }

  @Override
  public void awaitTermination() throws Exception {
    job.waitForCompletion(true); // killed by ClusterProviderImpl before returning
  }

  @Override
  public void close() throws Exception {
    if (job != null) {
      job.killJob();
    }
  }
}
