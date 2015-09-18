/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.hadoop;

import com.streamsets.pipeline.ClusterBinding;
import com.streamsets.pipeline.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HadoopMapReduceBinding implements ClusterBinding {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopMapReduceBinding.class);
  private final String[] args;
  private Properties properties;
  private Job job;

  public HadoopMapReduceBinding(String[] args) {
    this.args = args;
  }

  private String getProperty(String name) {
    String val = Utils.checkArgumentNotNull(properties.getProperty(name),
      "Property " + name + " cannot be null").trim();
    LOG.info("Value of property: " + name + " is " + val);
    return val;
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
      String dataFormat = getProperty("dataFormat");
      String source = this.getClass().getSimpleName();
      for (Object key : properties.keySet()) {
        String realKey = String.valueOf(key);
        String value = getProperty(realKey);
        conf.set(realKey, value, source);
      }
      conf.set("mapred.child.java.opts", javaOpts);
      conf.setBoolean("mapreduce.map.speculative", false);
      conf.setBoolean("mapreduce.reduce.speculative", false);
      if (dataFormat.equalsIgnoreCase("AVRO")) {
        conf.set(Job.INPUT_FORMAT_CLASS_ATTR, "org.apache.avro.mapreduce.AvroKeyInputFormat");
        conf.set(Job.MAP_OUTPUT_KEY_CLASS, "org.apache.avro.mapred.AvroKey");
      }
      job = Job.getInstance(conf, "StreamSets Data Collector - Batch Execution Mode");
      job.setJarByClass(this.getClass());
      job.setNumReduceTasks(0);
      if (!dataFormat.equalsIgnoreCase("AVRO")) {
        job.setOutputKeyClass(NullWritable.class);
      }
      job.setMapperClass(PipelineMapper.class);

      job.setOutputValueClass(NullWritable.class);

      job.setOutputFormatClass(NullOutputFormat.class);
    }
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
