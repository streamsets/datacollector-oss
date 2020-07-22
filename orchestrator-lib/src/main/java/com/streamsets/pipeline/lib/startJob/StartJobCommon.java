/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.startJob;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.Constants;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.startPipeline.Groups;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineCommon;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class StartJobCommon {

  private static final Logger LOG = LoggerFactory.getLogger(StartJobCommon.class);
  private final StartJobConfig conf;
  private ErrorRecordHandler errorRecordHandler;
  private ELVars jobIdConfigVars;
  private ELEval jobIdEval;
  private ELEval runtimeParametersEval;

  public StartJobCommon(StartJobConfig conf) {
    this.conf = conf;
  }

  public List<Stage.ConfigIssue> init(
      List<Stage.ConfigIssue> issues,
      ErrorRecordHandler errorRecordHandler,
      Stage.Context context
  ) {
    this.errorRecordHandler = errorRecordHandler;
    jobIdConfigVars = context.createELVars();
    jobIdEval = context.createELEval("jobId");
    runtimeParametersEval = context.createELEval("runtimeParameters");
    if (!conf.baseUrl.endsWith("/")) {
      conf.baseUrl += "/";
    }
    if (conf.tlsConfig.isEnabled()) {
      conf.tlsConfig.init(
          context,
          Groups.TLS.name(),
          StartPipelineCommon.SSL_CONFIG_PREFIX,
          issues
      );
    }

    if (!conf.jobTemplate && CollectionUtils.isNotEmpty(conf.jobIdConfigList)) {
      int index = 1;
      for (JobIdConfig jobIdConfig: conf.jobIdConfigList) {
        if (StringUtils.isEmpty(jobIdConfig.jobId)) {
          Stage.ConfigIssue issue = context.createConfigIssue(
              com.streamsets.pipeline.lib.startJob.Groups.JOB.name(),
              "conf.jobIdConfigList",
              StartJobErrors.START_JOB_06,
              index
          );
          issues.add(issue);
          break;
        }
        index++;
      }
    }

    return issues;
  }

  public List<CompletableFuture<Field>> getStartJobFutures(Executor executor, Record record) {
    List<CompletableFuture<Field>> startJobFutures = new ArrayList<>();
    if (conf.jobTemplate) {
      if (record != null) {
        RecordEL.setRecordInContext(jobIdConfigVars, record);
      }
      TimeNowEL.setTimeNowInContext(jobIdConfigVars, new Date());
      String templateJobId = jobIdEval.eval(
          jobIdConfigVars,
          conf.templateJobId,
          String.class
      );
      String runtimeParametersList = runtimeParametersEval.eval(
          jobIdConfigVars,
          conf.runtimeParametersList,
          String.class
      );
      CompletableFuture<Field> future = CompletableFuture.supplyAsync(new StartJobTemplateSupplier(
          conf,
          templateJobId,
          runtimeParametersList,
          errorRecordHandler
      ), executor);
      startJobFutures.add(future);
    } else {
      for(JobIdConfig jobIdConfig: conf.jobIdConfigList) {
        JobIdConfig resolvedJobIdConfig = new JobIdConfig();
        resolvedJobIdConfig.jobIdType = jobIdConfig.jobIdType;
        if (record != null) {
          RecordEL.setRecordInContext(jobIdConfigVars, record);
        }
        TimeNowEL.setTimeNowInContext(jobIdConfigVars, new Date());
        resolvedJobIdConfig.jobId = jobIdEval.eval(
            jobIdConfigVars,
            jobIdConfig.jobId,
            String.class
        );
        resolvedJobIdConfig.runtimeParameters = runtimeParametersEval.eval(
            jobIdConfigVars,
            jobIdConfig.runtimeParameters,
            String.class
        );
        CompletableFuture<Field> future = CompletableFuture.supplyAsync(new StartJobSupplier(
            conf,
            resolvedJobIdConfig,
            errorRecordHandler
        ), executor);
        startJobFutures.add(future);
      }
    }
    return startJobFutures;
  }

  public LinkedHashMap<String, Field> startJobInParallel(
      List<CompletableFuture<Field>> startJobFutures
  ) throws ExecutionException, InterruptedException {
    // Create a combined Future using allOf()
    CompletableFuture<Void> allFutures = CompletableFuture.allOf(
        startJobFutures.toArray(new CompletableFuture[0])
    );

    CompletableFuture<LinkedHashMap<String, Field>> completableFuture = allFutures.thenApply(v -> {
      LinkedHashMap<String, Field> jobResults = new LinkedHashMap<>();
      List<Field> jobIds = new ArrayList<>();
      boolean success = true;
      for (CompletableFuture<Field> future: startJobFutures) {
        try {
          Field startJobOutputField = future.get();
          if (startJobOutputField != null) {
            LinkedHashMap<String, Field> fields = startJobOutputField.getValueAsListMap();
            Field templateJobInstances = fields.get(Constants.TEMPLATE_JOB_INSTANCES_FIELD);
            if (templateJobInstances != null) {
              // Job Template
              List<Field> templateJobInstanceIdsList = templateJobInstances.getValueAsList();
              for (Field templateJobInstanceField : templateJobInstanceIdsList) {
                LinkedHashMap<String, Field> templateJobInstanceFieldValue =
                    templateJobInstanceField.getValueAsListMap();
                Field jobIdField = templateJobInstanceFieldValue.get(Constants.JOB_ID_FIELD);
                if (jobIdField != null) {
                  jobIds.add(Field.create(jobIdField.getValueAsString()));
                  jobResults.put(jobIdField.getValueAsString(), templateJobInstanceField);
                }
                if (!conf.runInBackground) {
                  Field finishedSuccessfully = templateJobInstanceFieldValue.get(Constants.FINISHED_SUCCESSFULLY_FIELD);
                  if (finishedSuccessfully != null) {
                    success &= finishedSuccessfully.getValueAsBoolean();
                  }
                }
              }
            } else {
              // Regular Job
              Field jobIdField = fields.get(Constants.JOB_ID_FIELD);
              if (jobIdField != null) {
                jobIds.add(Field.create(jobIdField.getValueAsString()));
                jobResults.put(jobIdField.getValueAsString(), startJobOutputField);
              }
              if (!conf.runInBackground) {
                Field finishedSuccessfully = fields.get(Constants.FINISHED_SUCCESSFULLY_FIELD);
                if (finishedSuccessfully != null) {
                  success &= finishedSuccessfully.getValueAsBoolean();
                }
              }
            }
          } else {
            success = false;
          }
        } catch (Exception ex) {
          LOG.error(ex.toString(), ex);
          errorRecordHandler.onError(StartJobErrors.START_JOB_08, ex.toString(), ex);
        }
      }
      LinkedHashMap<String, Field> outputField = new LinkedHashMap<>();
      outputField.put(Constants.JOB_IDS_FIELD, Field.create(jobIds));
      outputField.put(Constants.JOB_RESULTS_FIELD, Field.createListMap(jobResults));
      if (!conf.runInBackground) {
        // Don't set success flag for the task if the runInBackground set
        outputField.put(Constants.SUCCESS_FIELD, Field.create(success));
      }
      return outputField;
    });

    return completableFuture.get();
  }

}
