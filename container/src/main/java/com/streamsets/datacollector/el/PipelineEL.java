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
package com.streamsets.datacollector.el;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.Stage;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PipelineEL {

  private static final String PIPELINE_EL_PREFIX = "pipeline";
  public static final String DEFAULT_VALUE = "UNDEFINED";

  public static final String SDC_PIPELINE_TITLE_VAR = "SDC_PIPELINE_TITLE";
  public static final String SDC_PIPELINE_USER_VAR = "SDC_PIPELINE_USER";
  public static final String SDC_PIPELINE_NAME_VAR = "SDC_PIPELINE_NAME";
  public static final String SDC_PIPELINE_VERSION_VAR = "SDC_PIPELINE_VERSION";
  public static final String SDC_PIPELINE_START_TIME_VAR = "SDC_PIPELINE_START_TIME";

  @VisibleForTesting
  static final String PIPELINE_VERSION_VAR = "dpm.pipeline.version";

  private static final ThreadLocal<Map<String, Object>> CONSTANTS_IN_SCOPE_TL = ThreadLocal.withInitial(HashMap::new);

  @ElFunction(
    prefix = PIPELINE_EL_PREFIX,
    name = "name",
    description = "Returns the name of the pipeline")
  @Deprecated
  public static String name() {
    return (String)getVariableFromScope(SDC_PIPELINE_NAME_VAR);
  }

  @ElFunction(
    prefix = PIPELINE_EL_PREFIX,
    name = "version",
    description = "Returns the version of the pipeline if applicable. Returns \"UNDEFINED\" otherwise")
  public static String version() {
    return (String)getVariableFromScope(SDC_PIPELINE_VERSION_VAR);
  }

  @ElFunction(
    prefix = PIPELINE_EL_PREFIX,
    name = "title",
    description = "Returns the title of the pipeline if applicable. Returns \"UNDEFINED\" otherwise")
  public static String title() {
    return (String)getVariableFromScope(SDC_PIPELINE_TITLE_VAR);
  }

  @ElFunction(
    prefix = PIPELINE_EL_PREFIX,
    name = "id",
    description = "Returns the id of the pipeline if applicable. Returns \"UNDEFINED\" otherwise")
  public static String id() {
    return (String)getVariableFromScope(SDC_PIPELINE_NAME_VAR);
  }

  @ElFunction(
    prefix = PIPELINE_EL_PREFIX,
    name = "user",
    description = "Returns user who started this pipeline.")
  public static String user() {
    return (String)getVariableFromScope(SDC_PIPELINE_USER_VAR);
  }

  @ElFunction(
      prefix = PIPELINE_EL_PREFIX,
      name = "startTime",
      description = "Returns the start time of the pipeline as a datetime value."
  )
  public static Date startTime() {
    Map<String, Object> variablesInScope = CONSTANTS_IN_SCOPE_TL.get();
    if (variablesInScope.containsKey(SDC_PIPELINE_START_TIME_VAR)) {
      return (Date)variablesInScope.get(SDC_PIPELINE_START_TIME_VAR);
    }

    return new Date();
  }

  private static Object getVariableFromScope(String varName) {
    Map<String, Object> variablesInScope = CONSTANTS_IN_SCOPE_TL.get();
    Object name = DEFAULT_VALUE;
    if (variablesInScope.containsKey(varName)) {
      name = variablesInScope.get(varName);
    }
    return name;
  }

  public static void setConstantsInContext(
      PipelineConfiguration pipelineConfiguration,
      Stage.UserContext userContext,
      long startTime
  ) {
    String version = DEFAULT_VALUE;
    String title = DEFAULT_VALUE;
    String id = DEFAULT_VALUE;
    String user = Optional.ofNullable(userContext.getUser()).orElse(DEFAULT_VALUE);

    if (null != pipelineConfiguration.getInfo() && null != pipelineConfiguration.getInfo().getTitle()) {
      title = pipelineConfiguration.getInfo().getTitle();
    }
    if (null != pipelineConfiguration.getInfo() && null != pipelineConfiguration.getInfo().getPipelineId()) {
      id = pipelineConfiguration.getInfo().getPipelineId();
    }
    if (null != pipelineConfiguration.getMetadata() &&
        pipelineConfiguration.getMetadata().containsKey(PIPELINE_VERSION_VAR)) {
      version = pipelineConfiguration.getMetadata().get(PIPELINE_VERSION_VAR).toString();
    }
    Map<String, Object> variablesInScope = CONSTANTS_IN_SCOPE_TL.get();
    variablesInScope.put(PipelineEL.SDC_PIPELINE_VERSION_VAR, version);
    variablesInScope.put(PipelineEL.SDC_PIPELINE_NAME_VAR, id);
    variablesInScope.put(PipelineEL.SDC_PIPELINE_TITLE_VAR, title);
    variablesInScope.put(PipelineEL.SDC_PIPELINE_USER_VAR, user);
    variablesInScope.put(PipelineEL.SDC_PIPELINE_START_TIME_VAR, new Date(startTime));
    CONSTANTS_IN_SCOPE_TL.set(variablesInScope);
  }

  public static void unsetConstantsInContext() {
    Map<String, Object>  variablesInScope = CONSTANTS_IN_SCOPE_TL.get();
    variablesInScope.remove(PipelineEL.SDC_PIPELINE_VERSION_VAR);
    variablesInScope.remove(PipelineEL.SDC_PIPELINE_NAME_VAR);
    variablesInScope.remove(PipelineEL.SDC_PIPELINE_TITLE_VAR);
    variablesInScope.remove(PipelineEL.SDC_PIPELINE_USER_VAR);
    variablesInScope.remove(PipelineEL.SDC_PIPELINE_START_TIME_VAR);
    CONSTANTS_IN_SCOPE_TL.set(variablesInScope);
  }
}
