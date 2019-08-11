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

import com.google.common.base.Splitter;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.pipeline.api.ExecutionMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class StageLibraryDefinition {

  public static final String STAGE_WILDCARD = "*";
  public static final String CLUSTER_CONFIG_CLUSTER_TYPES = "clusterConfig.clusterTypes";

  public static String getExecutionModePrefix() {
    String prefix = "execution.mode_";
    if (Boolean.getBoolean("streamsets.cloud")) {
      prefix = "cloud." + prefix;
    }
    return prefix;
  }

  private ClassLoader classLoader;
  private String name;
  private String label;
  private Map<String, List<ExecutionMode>> stagesExecutionMode;
  private List<SparkClusterType> clusterTypes;
  private String version;

  private final List<Class> elDefs;
  private final List<ElFunctionDefinition> elFunctionDefinitions;
  private final List<ElConstantDefinition> elConstantDefinitions;
  private final List<String> elFunctionDefinitionsIdx;
  private final List<String> elConstantDefinitionsIdx;

  public StageLibraryDefinition(ClassLoader classLoader, String name, String label, Properties props, Class[] elDefs,
      List<ElFunctionDefinition> elFunctionDefinitions, List<ElConstantDefinition> elConstantDefinitions) {
    this.classLoader =  classLoader;
    this.name =  name;
    this.label = label;
    this.stagesExecutionMode = new HashMap<>();
    String executionModePrefix = getExecutionModePrefix();
    String allValue = props.getProperty(executionModePrefix + STAGE_WILDCARD, null);
    if (allValue != null) {
      List<ExecutionMode> executionModesForAll = (allValue == null) ? null : parseExecutionModes(allValue);
      stagesExecutionMode.put(STAGE_WILDCARD, executionModesForAll);;
    } else {
      for (Map.Entry entry : props.entrySet()) {
        String key = (String) entry.getKey();
        if (key.startsWith(executionModePrefix)) {
          String className = key.substring(executionModePrefix.length());
          String value = (String) entry.getValue();
          stagesExecutionMode.put(className, parseExecutionModes(value));
        }
      }
    }
    this.elDefs = new ArrayList<>();
    if (elDefs != null) {
      Collections.addAll(this.elDefs, elDefs);
    }
    elFunctionDefinitions = (elFunctionDefinitions == null)
                            ? Collections.<ElFunctionDefinition>emptyList() : elFunctionDefinitions;
    elConstantDefinitions = (elConstantDefinitions == null)
                            ? Collections.<ElConstantDefinition>emptyList() : elConstantDefinitions;
    this.elFunctionDefinitions = elFunctionDefinitions;
    elFunctionDefinitionsIdx = new ArrayList<>();
    for (ElFunctionDefinition f : elFunctionDefinitions) {
      elFunctionDefinitionsIdx.add(f.getIndex());
    }
    this.elConstantDefinitions = elConstantDefinitions;
    elConstantDefinitionsIdx = new ArrayList<>();
    for (ElConstantDefinition c : elConstantDefinitions) {
      elConstantDefinitionsIdx.add(c.getIndex());
    }

    this.clusterTypes = new ArrayList<>();
    for (Map.Entry entry : props.entrySet()) {
      String key = (String) entry.getKey();
      if (key.equals(CLUSTER_CONFIG_CLUSTER_TYPES)) {
        String value = (String) entry.getValue();
        clusterTypes.addAll(parseClusterTypes(value));
      }
    }
  }

  List<ExecutionMode> parseExecutionModes(String value) {
    return Splitter.on(",")
        .trimResults()
        .omitEmptyStrings()
        .splitToList(value)
        .stream()
        .map(ExecutionMode::valueOf)
        .collect(Collectors.toList());
  }

  List<SparkClusterType> parseClusterTypes(String value) {
    return Splitter.on(",")
        .trimResults()
        .omitEmptyStrings()
        .splitToList(value)
        .stream()
        .map(SparkClusterType::valueOf)
        .collect(Collectors.toList());
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public String getVersion() {
    return version;
  }

  // Ideally this should be immutable and in constructor, but this has been added later
  public void setVersion(String version) {
    this.version = version;
  }

  public String getName() {
    return name;
  }

  public String getLabel() {
    return label;
  }

  public List<ExecutionMode> getStageExecutionModesOverride(Class klass) {
    return (stagesExecutionMode.containsKey(STAGE_WILDCARD))
        ? stagesExecutionMode.get(STAGE_WILDCARD)
        : stagesExecutionMode.get(klass.getName());
  }

  public List<Class> getElDefs() {
    return elDefs;
  }

  public List<ElFunctionDefinition> getElFunctionDefinitions() {
    return elFunctionDefinitions;
  }

  public List<ElConstantDefinition> getElConstantDefinitions() {
    return elConstantDefinitions;
  }

  public List<String> getElFunctionDefinitionsIdx() {
    return elFunctionDefinitionsIdx;
  }

  public List<String> getElConstantDefinitionsIdx() {
    return elConstantDefinitionsIdx;
  }

  public List<SparkClusterType> getClusterTypes() {
    return clusterTypes;
  }
}
