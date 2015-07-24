/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.google.common.base.Splitter;
import com.streamsets.pipeline.api.ExecutionMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class StageLibraryDefinition {
  private static final String EXECUTION_MODE_PREFIX = "execution.mode_";
  private static final String CLUSTER_MODE_JAR_BLACKLIST = "cluster.jar.blacklist_";

  private ClassLoader classLoader;
  private String name;
  private String label;
  private Map<String, List<ExecutionMode>> stagesExecutionMode;
  private Map<String, List<String>> clusterModeJarBlacklist;

  public StageLibraryDefinition(ClassLoader classLoader, String name, String label, Properties props) {
    this.classLoader =  classLoader;
    this.name =  name;
    this.label = label;
    this.stagesExecutionMode = new HashMap<>();
    this.clusterModeJarBlacklist = new HashMap<>();
    for (Map.Entry entry : props.entrySet()) {
      String key = (String) entry.getKey();
      if (key.startsWith(EXECUTION_MODE_PREFIX)) {
        String className = key.substring(EXECUTION_MODE_PREFIX.length());
        String value = (String) entry.getValue();
        List<ExecutionMode> executionModes = new ArrayList<>();
        for (String mode : Splitter.on(",").trimResults().omitEmptyStrings().split(value)) {
          executionModes.add(ExecutionMode.valueOf(mode.trim()));
        }
        stagesExecutionMode.put(className, executionModes);
      } else if (key.startsWith(CLUSTER_MODE_JAR_BLACKLIST)) {
        String className = key.substring(CLUSTER_MODE_JAR_BLACKLIST.length());
        String value = (String) entry.getValue();
        clusterModeJarBlacklist.put(className, Splitter.on(",").trimResults().omitEmptyStrings().splitToList(value));
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

  public List<String> getClusterModeJarBlacklist(String klassName) {
    if (clusterModeJarBlacklist.containsKey(klassName)) {
      return clusterModeJarBlacklist.get(klassName);
    }
    return Collections.emptyList();
  }

  public List<ExecutionMode> getStageExecutionModesOverride(Class klass) {
    return stagesExecutionMode.get(klass.getName());
  }

}
