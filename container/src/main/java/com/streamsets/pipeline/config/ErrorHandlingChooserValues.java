/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;

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
          list.add(def.getLabel() + " - " + def.getLibraryLabel());
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
