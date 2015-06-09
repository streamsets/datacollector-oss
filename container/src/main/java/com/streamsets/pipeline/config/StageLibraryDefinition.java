/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.ExecutionMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class StageLibraryDefinition {
  private static final String EXECUTION_MODE_PREFIX = "execution.mode_";

  private ClassLoader classLoader;
  private String name;
  private String label;
  private Map<String, List<ExecutionMode>> stagesExecutionMode;

  public StageLibraryDefinition(ClassLoader classLoader, String name, String label, Properties props) {
    this.classLoader =  classLoader;
    this.name =  name;
    this.label = label;
    this.stagesExecutionMode = new HashMap<>();
    for (Map.Entry entry : props.entrySet()) {
      String key = (String) entry.getKey();
      if (key.startsWith(EXECUTION_MODE_PREFIX)) {
        String className = key.substring(EXECUTION_MODE_PREFIX.length());
        String value = (String) entry.getValue();
        List<ExecutionMode> executionModes = new ArrayList<>();
        for (String mode : value.split(",")) {
          executionModes.add(ExecutionMode.valueOf(mode.trim()));
        }
        stagesExecutionMode.put(className, executionModes);
      }
    }

  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public String getName() {
    return name;
  }

  public String getLabel() {
    return label;
  }

  public List<ExecutionMode> getStageExecutionModesOverride(Class klass) {
    return stagesExecutionMode.get(klass.getName());
  }

}
