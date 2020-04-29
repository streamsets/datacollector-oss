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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;

import java.util.LinkedHashMap;

public class CommonUtil {

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
}
