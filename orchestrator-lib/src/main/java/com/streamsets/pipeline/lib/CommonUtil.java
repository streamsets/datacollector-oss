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
package com.streamsets.pipeline.lib;

import com.streamsets.datacollector.client.model.MetricRegistryJson;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class CommonUtil {

  private static final String PIPELINE_PREFIX = "pipeline.";
  private static final String STAGE_PREFIX = "stage.";
  private static final String INPUT_RECORDS = ".inputRecords.";
  private static final String OUTPUT_RECORDS = ".outputRecords.";
  private static final String ERROR_RECORDS = ".errorRecords.";
  private static final String STAGE_ERRORS = ".stageErrors.";
  private static final String BATCH_INPUT_RECORDS = ".batchInputRecords.";
  private static final String BATCH_OUTPUT_RECORDS = ".batchOutputRecords.";
  private static final String BATCH_ERROR_RECORDS = ".batchErrorRecords.";
  private static final String BATCH_ERROR_MESSAGES = ".batchErrorMessages.";
  private static final String DOT_REGEX = "\\.";

  public static Record createOrchestratorTaskRecord(
      Record record,
      Stage.Context context,
      String taskName,
      LinkedHashMap<String, Field> outputField
  ) {
    if (record == null) {
      record = context.createRecord("orchestrator");

      LinkedHashMap<String, Field> orchestratorTasks = new LinkedHashMap<>();
      orchestratorTasks.put(taskName, Field.createListMap(outputField));

      LinkedHashMap<String, Field> newRootField = new LinkedHashMap<>();
      newRootField.put(Constants.ORCHESTRATOR_TASKS_FIELD_NAME, Field.createListMap(orchestratorTasks));
      record.set(Field.createListMap(newRootField));
    } else {
      Field orchestratorTasksField = record.get(Constants.ORCHESTRATOR_TASKS_FIELD_PATH);
      if (orchestratorTasksField == null) {
        orchestratorTasksField = Field.createListMap(new LinkedHashMap<>());

        Field rootField = record.get();
        if (rootField.getType() == Field.Type.LIST_MAP || rootField.getType() == Field.Type.MAP) {
          LinkedHashMap<String, Field> currentField = rootField.getValueAsListMap();
          currentField.put(Constants.ORCHESTRATOR_TASKS_FIELD_NAME, orchestratorTasksField);
        } else {
          LinkedHashMap<String, Field> newRootField = new LinkedHashMap<>();
          newRootField.put(Constants.ORCHESTRATOR_TASKS_FIELD_NAME, orchestratorTasksField);
          record.set(Field.createListMap(newRootField));
        }
      }

      LinkedHashMap<String, Field> orchestratorTasks = orchestratorTasksField.getValueAsListMap();
      orchestratorTasks.put(taskName, Field.createListMap(outputField));
    }

    return record;
  }

  public static Field getMetricsField(MetricRegistryJson metricRegistry) {
    LinkedHashMap<String, Field> metricsField = new LinkedHashMap<>();

    if (metricRegistry != null) {
      Map<String, Field> pipelineMetrics = new HashMap<>();
      Map<String, Map<String, Field>> stageMetricsMap = new HashMap<>();
      metricRegistry.getCounters().forEach((metricKey, counter) -> {
        Map<String, Field> field = null;

        if (metricKey.startsWith(PIPELINE_PREFIX)) {
          field = pipelineMetrics;
        } else if (metricKey.startsWith(STAGE_PREFIX)) {
          String stageId = metricKey.split(DOT_REGEX)[1];
          stageMetricsMap.computeIfAbsent(stageId, id -> new HashMap<>());
          field = stageMetricsMap.get(stageId);
        }

        if (field != null) {
          if (metricKey.contains(INPUT_RECORDS) || metricKey.contains(BATCH_INPUT_RECORDS)) {
            field.put("inputRecords", Field.create(counter.getCount()));
          } else if (metricKey.contains(OUTPUT_RECORDS) || metricKey.contains(BATCH_OUTPUT_RECORDS)) {
            field.put("outputRecords", Field.create(counter.getCount()));
          } else if (metricKey.contains(ERROR_RECORDS) || metricKey.contains(BATCH_ERROR_RECORDS)) {
            field.put("errorRecords", Field.create(counter.getCount()));
          } else if (metricKey.contains(STAGE_ERRORS) || metricKey.contains(BATCH_ERROR_MESSAGES)) {
            field.put("errorMessages", Field.create(counter.getCount()));
          }
        }
      });

      metricsField.put("pipeline", Field.create(pipelineMetrics));

      Map<String, Field> stageMetricsFieldMap = new HashMap<>();
      stageMetricsMap.forEach((k, v) -> stageMetricsFieldMap.put(k, Field.create(v)));
      metricsField.put("stages", Field.create(stageMetricsFieldMap));
    }

    return Field.create(metricsField);
  }
}
