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
package com.streamsets.datacollector.el;

import com.streamsets.pipeline.api.ElFunction;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JobEL {

  private static final String JOB_EL_PREFIX = "job";
  public static final String DEFAULT_VALUE = "UNDEFINED";
  public static final String JOB_ID_VAR = "JOB_ID";
  public static final String JOB_NAME_VAR = "JOB_NAME";
  static final String JOB_USER_VAR = "JOB_USER";
  static final String JOB_START_TIME_VAR = "JOB_START_TIME";
  private static final ThreadLocal<Map<String, Object>> CONSTANTS_IN_SCOPE_TL = ThreadLocal.withInitial(HashMap::new);

  @ElFunction(
      prefix = JOB_EL_PREFIX,
      name = "id",
      description = "Returns the id of the job if applicable. Returns \"UNDEFINED\" otherwise")
  public static String id() {
    return (String)getVariableFromScope(JOB_ID_VAR);
  }

  @ElFunction(
    prefix = JOB_EL_PREFIX,
    name = "name",
    description = "Returns the name of the job if applicable. Returns \"UNDEFINED\" otherwise")
  public static String name() {
    return (String)getVariableFromScope(JOB_NAME_VAR);
  }

  @ElFunction(
    prefix = JOB_EL_PREFIX,
    name = "user",
    description = "Returns user who started this job. Returns \"UNDEFINED\" otherwise")
  public static String user() {
    return (String)getVariableFromScope(JOB_USER_VAR);
  }

  @ElFunction(
      prefix = JOB_EL_PREFIX,
      name = "startTime",
      description = "Returns start time of job or pipeline as a datetime value."
  )
  public static Date startTime() {
    return (Date)getVariableFromScope(JOB_START_TIME_VAR);
  }

  private static Object getVariableFromScope(String varName) {
    Map<String, Object> variablesInScope = CONSTANTS_IN_SCOPE_TL.get();
    Object name = DEFAULT_VALUE;
    if (variablesInScope.containsKey(varName)) {
      name = variablesInScope.get(varName);
    }
    return name;
  }

  public static void setConstantsInContext(Map<String, Object> parameters) {
    String id = DEFAULT_VALUE;
    String name = DEFAULT_VALUE;
    String user = DEFAULT_VALUE;
    long startTime = new Date().getTime();

    if (parameters != null) {
      if (parameters.containsKey(JOB_ID_VAR)) {
        id = (String)parameters.get(JOB_ID_VAR);
      }

      if (parameters.containsKey(JOB_NAME_VAR)) {
        name = (String)parameters.get(JOB_NAME_VAR);
      }

      if (parameters.containsKey(JOB_USER_VAR)) {
        user = (String)parameters.get(JOB_USER_VAR);
      }

      if (parameters.containsKey(JOB_START_TIME_VAR)) {
        startTime = (Long)parameters.get(JOB_START_TIME_VAR);
      }
    }

    Map<String, Object> variablesInScope = CONSTANTS_IN_SCOPE_TL.get();
    variablesInScope.put(JobEL.JOB_ID_VAR, id);
    variablesInScope.put(JobEL.JOB_NAME_VAR, name);
    variablesInScope.put(JobEL.JOB_USER_VAR, user);
    variablesInScope.put(JobEL.JOB_START_TIME_VAR, new Date(startTime));
    CONSTANTS_IN_SCOPE_TL.set(variablesInScope);
  }

  public static void unsetConstantsInContext() {
    Map<String, Object>  variablesInScope = CONSTANTS_IN_SCOPE_TL.get();
    variablesInScope.remove(JobEL.JOB_ID_VAR);
    variablesInScope.remove(JobEL.JOB_NAME_VAR);
    variablesInScope.remove(JobEL.JOB_USER_VAR);
    variablesInScope.remove(JobEL.JOB_START_TIME_VAR);
    CONSTANTS_IN_SCOPE_TL.set(variablesInScope);
  }
}
