/**
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.pipeline.executor.spark;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.pipeline.executor.spark.yarn.YarnAppLauncher;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.event.EventCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_00;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_06;

public class SparkExecutor extends BaseExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutor.class);
  public static final String APP_SUBMITTED_EVENT = "AppSubmittedEvent";
  public static final String APP_ID = "app-id";

  private final SparkExecutorConfigBean configBean;
  private ELEval elEval;
  private ELVars elVars;
  private AppLauncher appLauncher;

  /**
   * Issued for every submitted Spark job.
   */
  public static final EventCreator JOB_CREATED = new EventCreator.Builder(APP_SUBMITTED_EVENT, 1)
      .withRequiredField(APP_ID)
      .build();

  public SparkExecutor(SparkExecutorConfigBean conf) {
    this.configBean = conf;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    try {
      elVars = getContext().createELVars();
      elEval = getContext().createELEval("appArgs");
      for (String el : configBean.appArgs) {
        getContext().parseEL(el);
      }
    } catch (ELEvalException ex) {
      LOG.error("Error evaluating EL", ex);
      issues.add(getContext().createConfigIssue(
          "APPLICATION_GROUP", "conf.appArgs", ex.getErrorCode(), ex.getParams()));
    }
    appLauncher = getLauncher();
    Optional.ofNullable(appLauncher.init(getContext(), configBean)).ifPresent(issues::addAll);
    return issues;
  }

  @Override
  public void destroy() {
    // Nothing to cleanup.
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    while(records.hasNext()) {
      Record record = records.next();
      try {
        Optional<String> appIdOpt = appLauncher.launchApp(evaluateArgsELs(record));
        appIdOpt.ifPresent(
            (String appId) -> {
              LOG.info(Utils.format("Spark application launched with app id: '{}'", appId));
              JOB_CREATED.create(getContext()).with(APP_ID, appId).createAndSend();
            }
        );
        if (!appLauncher.waitForCompletion()) {
          LOG.info("Spark app has been submitted, but it has not completed yet");
        }
      } catch (ApplicationLaunchFailureException ex) {
        throw new StageException(SPARK_EXEC_00, ex.getMessage(), ex);
      } catch(InterruptedException ex) { //NOSONAR
        LOG.error(SPARK_EXEC_06.getMessage(), ex);
        throw new StageException(SPARK_EXEC_06, ex.getMessage(), ex);
      }
    }
  }

  @VisibleForTesting
  protected AppLauncher getLauncher() {
    switch (configBean.clusterManager) { //NOSONAR
      case YARN:
        return new YarnAppLauncher();
      default:
        throw new IllegalArgumentException(
            Utils.format("{} is not a valid cluster manager", configBean.clusterManager));
    }
  }

  private List<String> evaluateArgsELs(Record record) throws ELEvalException {
    RecordEL.setRecordInContext(elVars, record);
    List<String> evaluatedArgs = new ArrayList<>(configBean.appArgs.size());
    for (String arg : configBean.appArgs) {
      evaluatedArgs.add(elEval.eval(elVars, arg, String.class));
    }
    return evaluatedArgs;
  }
}
