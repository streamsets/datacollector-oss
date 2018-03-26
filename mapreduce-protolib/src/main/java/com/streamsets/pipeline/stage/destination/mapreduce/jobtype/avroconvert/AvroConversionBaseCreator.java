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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.util.concurrent.Callable;

public abstract class AvroConversionBaseCreator implements Configurable, Callable<Job> {

  private Configuration conf;

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  protected abstract void addNecessaryJarsToJob(Configuration conf);
  protected abstract Class<? extends InputFormat> getInputFormatClass();
  protected abstract Class<? extends Mapper> getMapperClass();

  @Override
  public Job call() throws Exception {
    // We're explicitly disabling speculative execution
    conf.set("mapreduce.map.speculative", "false");
    conf.set("mapreduce.map.maxattempts", "1");

    conf.set("mapreduce.job.user.classpath.first", "true");
    conf.set("mapreduce.task.classpath.user.precedence", "true");
    conf.set("mapreduce.task.classpath.first", "true");

    addNecessaryJarsToJob(conf);

    Job job = Job.getInstance(conf);

    // IO formats
    job.setInputFormatClass(getInputFormatClass());
    job.setOutputFormatClass(NullOutputFormat.class);

    // Mapper & job output
    job.setMapperClass(getMapperClass());
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    // It's map only job
    job.setNumReduceTasks(0);

    // General configuration
    job.setJarByClass(getClass());


    return job;
  }

}
