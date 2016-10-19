/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.datacollector.el;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.pipeline.api.ElFunction;

import java.util.HashMap;
import java.util.Map;

public class PipelineEL {

  private static final String PIPELINE_EL_PREFIX = "pipeline";
  public static final String SDC_PIPELINE_NAME_VAR = "SDC_PIPELINE_NAME";
  public static final String SDC_PIPELINE_VERSION_VAR = "SDC_PIPELINE_VERSION";

  @VisibleForTesting
  static final String PIPELINE_VERSION_VAR = "dpm.pipeline.version";

  private static final ThreadLocal<Map<String, Object>> CONSTANTS_IN_SCOPE_TL = new ThreadLocal() {
    @Override public Map<String, Object> initialValue() {
      return new HashMap<>();
    }
  };

  @ElFunction(
    prefix = PIPELINE_EL_PREFIX,
    name = "name",
    description = "Returns the name of the pipeline")
  public static String name() {
    return getVariableFromScope(SDC_PIPELINE_NAME_VAR);
  }

  @ElFunction(
    prefix = PIPELINE_EL_PREFIX,
    name = "version",
    description = "Returns the version of the pipeline if applicable. Returns \"UNDEFINED\" otherwise")
  public static String version() {
    return getVariableFromScope(SDC_PIPELINE_VERSION_VAR);
  }

  private static String getVariableFromScope(String varName) {
    Map<String, Object> variablesInScope = CONSTANTS_IN_SCOPE_TL.get();
    String name = "UNDEFINED";
    if (variablesInScope.containsKey(varName)) {
      name = (String) variablesInScope.get(varName);
    }
    return name;
  }

  public static void setConstantsInContext(PipelineConfiguration pipelineConfiguration) {
    String name = "UNDEFINED";
    String version = "UNDEFINED";

    if (null != pipelineConfiguration.getInfo() && null != pipelineConfiguration.getInfo().getName()) {
      name = pipelineConfiguration.getInfo().getName();
    }
    if (null != pipelineConfiguration.getMetadata() &&
        pipelineConfiguration.getMetadata().containsKey(PIPELINE_VERSION_VAR)) {
      version = (String) pipelineConfiguration.getMetadata().get(PIPELINE_VERSION_VAR);
    }
    Map<String, Object> variablesInScope = CONSTANTS_IN_SCOPE_TL.get();
    variablesInScope.put(PipelineEL.SDC_PIPELINE_NAME_VAR, name);
    variablesInScope.put(PipelineEL.SDC_PIPELINE_VERSION_VAR, version);
    CONSTANTS_IN_SCOPE_TL.set(variablesInScope);
  }

  public static void unsetConstantsInContext() {
    Map<String, Object>  variablesInScope = CONSTANTS_IN_SCOPE_TL.get();
    variablesInScope.remove(PipelineEL.SDC_PIPELINE_NAME_VAR);
    variablesInScope.remove(PipelineEL.SDC_PIPELINE_VERSION_VAR);
    CONSTANTS_IN_SCOPE_TL.set(variablesInScope);
  }
}