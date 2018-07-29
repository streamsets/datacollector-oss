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
package com.streamsets.datacollector.config;

import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.StageType;

import java.util.ArrayList;
import java.util.List;

public class PipelineTestStageChooserValues implements ChooserValues {
  private static List<String> values;
  private static List<String> labels;

  private static List<String> getOptions(List<StageDefinition> stageDefinitions, boolean value) {
    List<String> list = new ArrayList<>();
    for (StageDefinition def : stageDefinitions) {
      if (value) {
        list.add(def.getLibrary() + "::" + def.getName() + "::" + def.getVersion());
      } else {
        list.add(def.getLabel() + " (Library: " + def.getLibraryLabel() + ")");
      }
    }
    return list;
  }

  public static void setHandlingOptions(StageLibraryTask stageLibraryTask) {
    List<StageDefinition> stageDefinitions = new ArrayList<>();
    for (StageDefinition def : stageLibraryTask.getStages()) {
      if (def.getType().equals(StageType.SOURCE)) {
        stageDefinitions.add(def);
      }
    }

    stageDefinitions.sort((o1, o2) -> o1.getLabel().compareToIgnoreCase(o2.getLabel()));

    PipelineTestStageChooserValues.values = getOptions(stageDefinitions, true);
    PipelineTestStageChooserValues.labels = getOptions(stageDefinitions, false);
  }

  @Override
  public String getResourceBundle() {
    return null;
  }

  @Override
  public List<String> getValues() {
    return values;
  }

  @Override
  public List<String> getLabels() {
    return labels;
  }
}
