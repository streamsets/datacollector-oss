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

import com.google.common.annotations.VisibleForTesting;
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

public class StageLibraryDefinition {
  @VisibleForTesting
  public static final String EXECUTION_MODE_PREFIX = "execution.mode_";

  private ClassLoader classLoader;
  private String name;
  private String label;
  private Map<String, List<ExecutionMode>> stagesExecutionMode;

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

}
