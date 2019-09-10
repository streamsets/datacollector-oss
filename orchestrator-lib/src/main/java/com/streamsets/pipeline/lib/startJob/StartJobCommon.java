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
import com.streamsets.pipeline.api.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class StartJobCommon {

  private static final Logger LOG = LoggerFactory.getLogger(StartJobCommon.class);
  private StartJobConfig conf;

  public StartJobCommon(StartJobConfig conf) {
    this.conf = conf;
  }

  public List<Stage.ConfigIssue> init(List<Stage.ConfigIssue> issues) {
    if (!conf.baseUrl.endsWith("/")) {
      conf.baseUrl += "/";
    }
    return issues;
  }

  public LinkedHashMap<String, Field> startJobInParallel(
      List<CompletableFuture<Field>> startJobFutures,
      Stage.Context context
  ) throws ExecutionException, InterruptedException {
    // Create a combined Future using allOf()
    CompletableFuture<Void> allFutures = CompletableFuture.allOf(
        startJobFutures.toArray(new CompletableFuture[0])
    );

    CompletableFuture<LinkedHashMap<String, Field>> completableFuture = allFutures.thenApply(v -> {
      LinkedHashMap<String, Field> outputField = new LinkedHashMap<>();
      boolean success = true;
      for (CompletableFuture<Field> future: startJobFutures) {
        try {
          Field startPipelineOutputField = future.get();
          if (startPipelineOutputField != null) {
            LinkedHashMap<String, Field> fields = startPipelineOutputField.getValueAsListMap();
            Field jobIdField = fields.get("jobId");
            if (jobIdField != null) {
              outputField.put(jobIdField.getValueAsString(), startPipelineOutputField);
            }
            success &= fields.get("success").getValueAsBoolean();
          }
        } catch (Exception ex) {
          LOG.error(ex.getMessage(), ex);
          context.reportError(ex);
        }
      }
      outputField.put("success", Field.create(success));
      return outputField;
    });

    return completableFuture.get();
  }

}
