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
package com.streamsets.datacollector.config;

import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.StageType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ErrorHandlingChooserValues implements ChooserValues {
  private static List<String> values;
  private static List<String> labels;

  private static List<String> getOptions(List<StageDefinition> errorStageDefinitions, boolean value) {
    List<String> list = new ArrayList<>();
    for (StageDefinition def : errorStageDefinitions) {
      if (value) {
        list.add(def.getLibrary() + "::" + def.getName() + "::" + def.getVersion());
      } else {
        list.add(def.getLabel() + " (Library: " + def.getLibraryLabel() + ")");
      }
    }
    return list;
  }

  public static void setErrorHandlingOptions(StageLibraryTask stageLibraryTask) {
    List<StageDefinition> errorStageDefinitions = new ArrayList<>();
    for (StageDefinition def : stageLibraryTask.getStages()) {
      if (def.getType() == StageType.TARGET && def.isErrorStage()) {
        errorStageDefinitions.add(def);
      }
    }

    Collections.sort(errorStageDefinitions, new Comparator<StageDefinition>() {
      @Override
      public int compare(StageDefinition o1, StageDefinition o2) {
        return o1.getLabel().compareToIgnoreCase(o2.getLabel());
      }
    });

    ErrorHandlingChooserValues.values = getOptions(errorStageDefinitions, true);
    ErrorHandlingChooserValues.labels = getOptions(errorStageDefinitions, false);
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
