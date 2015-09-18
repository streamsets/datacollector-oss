/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.datacollector.config;

import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.ChooserValues;

import java.util.ArrayList;
import java.util.List;

public class ErrorHandlingChooserValues implements ChooserValues {
  private static List<String> values;
  private static List<String> labels;

  private static List<String> getOptions(StageLibraryTask stageLibrary, boolean value) {
    List<String> list = new ArrayList<>();
    for (StageDefinition def : stageLibrary.getStages()) {
      if (def.getType() == StageType.TARGET && def.isErrorStage()) {
        if (value) {
          list.add(def.getLibrary() + "::" + def.getName() + "::" + def.getVersion());
        } else {
          list.add(def.getLabel() + " (Library: " + def.getLibraryLabel() + ")");
        }
      }
    }
    return list;
  }

  public static void setErrorHandlingOptions(StageLibraryTask stageLibraryTask) {
    ErrorHandlingChooserValues.values = getOptions(stageLibraryTask, true);
    ErrorHandlingChooserValues.labels = getOptions(stageLibraryTask, false);
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
