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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.util.concurrent.Callable;

/**
 * Simple JobCreator that doesn't do anything special and just takes given
 * configuration and turns it into job. It's expected that all MR related
 * information is already specified in the configuration job(e.g. mapper,
 * reducer, input/output formats, ...)
 */
public class SimpleJobCreator implements Configurable, Callable<Job> {

  Configuration conf;

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Job call() throws Exception {
    return Job.getInstance(conf);
  }
}
