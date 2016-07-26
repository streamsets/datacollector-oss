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
package com.streamsets.pipeline.stage.destination.mapreduce;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.mapreduce.config.JobConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.config.MapReduceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class MapReduceExecutor extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceExecutor.class);

  private final MapReduceConfig mapReduceConfig;
  private final JobConfig jobConfig;
  private ErrorRecordHandler errorRecordHandler;

  public MapReduceExecutor(MapReduceConfig mapReduceConfig, JobConfig jobConfig) {
    this.mapReduceConfig = mapReduceConfig;
    this.jobConfig = jobConfig;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    issues.addAll(mapReduceConfig.init(getContext(), "mapReduceConfig"));
    issues.addAll(jobConfig.init(getContext(), "jobConfig"));

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    ELVars variables = getContext().createELVars();
    ELEval eval = getContext().createELEval("jobConfigs");

    Iterator<Record> it = batch.getRecords();
    while(it.hasNext()) {
      final Record record = it.next();
      RecordEL.setRecordInContext(variables, record);

      // Job configuration object is a clone of the original one that we're keeping in mapReduceConfig class
      final Configuration jobConfiguration = new Configuration(mapReduceConfig.getConfiguration());

      // Evaluate all dynamic properties and store them in the configuration job
      for(Map.Entry<String, String> entry : jobConfig.jobConfigs.entrySet()) {
        String key = eval.eval(variables, entry.getKey(), String.class);
        String value = eval.eval(variables, entry.getValue(), String.class);

        jobConfiguration.set(key, value);
      }

      try {
        mapReduceConfig.getUGI().doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            // Create and submit MapReduce job
            Callable<Job> jobCreator = ReflectionUtils.newInstance(jobConfig.getJobCreator(), jobConfiguration);
            Job job = jobCreator.call();
            job.submit();

            // And generate event with further job details
            EventRecord event = getContext().createEventRecord("job-created", 1);
            Map<String, Field> eventMap = ImmutableMap.of(
              "tracking-url", Field.create(job.getTrackingURL()),
              "job-id", Field.create(job.getJobID().toString())
            );
            event.set(Field.create(eventMap));
            getContext().toEvent(event);
            return null;
          }
        });
      } catch (IOException|InterruptedException e) {
        LOG.error("Can't submit mapreduce job", e);
        errorRecordHandler.onError(new OnRecordErrorException(record, MapReduceErrors.MAPREDUCE_0005, e.getMessage()));
      }
    }
  }
}
