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
package com.streamsets.pipeline.stage.executor.emailexecutor;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.util.List;
import java.util.Map;

public class EmailConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Condition",
      description = "Boolean expression.  Triggers sending an email when true. ",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String condition;
  ELEval conditionELEval;
  ELVars conditionELVars;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Email IDs",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 200,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public List<String> email;
  Map<String, ELEval> emailELEval;
  Map<String, ELVars> emailELVars;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      label = "Email Subject",
      lines = 1,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 300,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String subject;
  ELEval subjectELEval;
  ELVars subjectELVars;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      lines = 5,
      label = "Email Body",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 400,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String body;
  ELEval bodyELEval;
  ELVars bodyELVars;

  // this will force vertical rendering.
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValue = "dummy",
      lines = 3,
      dependsOn = "body",
      label = "dummy",
      displayPosition = 500
  )
  public String dummy;
}
