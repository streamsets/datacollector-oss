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
package com.streamsets.datacollector.antennadoctor.engine.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Arrays;
import java.util.List;

public class StageIssueEL {
  private static final String EXCEPTION_VAR = "__content_stageIssue_exception";
  private static final String ERRORCODE_VAR = "__content_stageIssue_errorCode";
  private static final String ERRORMSG_VAR = "__content_stageIssue_errorMessage";
  private static final String ARGS_VAR = "__content_stageIssue_args";

  public static void setVars(ELVars variables, Exception e) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(EXCEPTION_VAR, e);
    variables.addContextVariable(ERRORMSG_VAR, e.toString());
  }

  public static void setVars(ELVars variables, String errorMessage) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(ERRORMSG_VAR, errorMessage);
  }

  public static void setVars(ELVars variables, ErrorCode errorCode, Object ...args) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(ERRORCODE_VAR, errorCode);
    variables.addContextVariable(ARGS_VAR, Arrays.asList(args));
  }

  @ElFunction(prefix = "issue", name = "exception")
  public static Exception exception() {
    return (Exception) ELEval.getVariablesInScope().getContextVariable(EXCEPTION_VAR);
  }

  @ElFunction(prefix = "issue", name = "errorCode")
  public static ErrorCode errorCode() {
    return (ErrorCode) ELEval.getVariablesInScope().getContextVariable(ERRORCODE_VAR);
  }

  @ElFunction(prefix = "issue", name = "errorMessage")
  public static String errorMessage() {
    return (String) ELEval.getVariablesInScope().getContextVariable(ERRORMSG_VAR);
  }

  @ElFunction(prefix = "issue", name = "args")
  public static List args() {
    return (List) ELEval.getVariablesInScope().getContextVariable(ARGS_VAR);
  }
}
