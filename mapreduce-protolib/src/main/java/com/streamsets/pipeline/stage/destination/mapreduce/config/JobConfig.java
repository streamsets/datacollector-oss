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
package com.streamsets.pipeline.stage.destination.mapreduce.config;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.destination.mapreduce.Groups;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceErrors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class JobConfig {
  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Job Name",
    description = "Name that will be used for naming the executed mapreduce job.",
    defaultValue = "SDC MapReduceJob",
    displayPosition = 10,
    group = "JOB"
  )
  public String jobName;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Job Type",
    description = "Type of job that will be executed.",
    displayPosition = 20,
    group = "JOB"
  )
  @ValueChooserModel(JobTypeChooserValues.class)
  public JobType jobType;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Custom JobCreator",
    description = "Custom class implementing JobCreator interface.",
    displayPosition = 30,
    group = "JOB",
    dependsOn = "jobType",
    triggeredByValue = "CUSTOM"
  )
  public String customJobCreator;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    label = "Job Configuration",
    description = "Specific configuration options for each job, evaluation expressions are allowed here.",
    displayPosition = 40,
    group = "JOB",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class}
  )
  public Map<String, String> jobConfigs;

  private Class<Callable<Job>> jobCreator;

  public Class<Callable<Job>> getJobCreator() {
    return jobCreator;
  }

  public List<Stage.ConfigIssue> init(Stage.Context context, String prefix) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Joiner joiner = Joiner.on(".");
    String creatorClassName = jobType == JobType.CUSTOM ? customJobCreator : jobType.getKlass();

    // Try loading the configured class
    Class creator;
    try {
      creator = Class.forName(creatorClassName);
    } catch (ClassNotFoundException e) {
      issues.add(context.createConfigIssue(
        Groups.JOB.name(),
        joiner.join(prefix, "customJobCreator"),
        MapReduceErrors.MAPREDUCE_0001,
        creatorClassName
      ));
      return issues;
    }

    // Verify that the configured class implements our required interface
    if(!Callable.class.isAssignableFrom(creator)) {
      issues.add(context.createConfigIssue(
        Groups.JOB.name(),
        joiner.join(prefix, "customJobCreator"),
        MapReduceErrors.MAPREDUCE_0004,
        creatorClassName
      ));
      return issues;
    }

    // We know that this is type safe due to checks above
    jobCreator = creator;

    return issues;
  }
}
