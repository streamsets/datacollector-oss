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
package com.streamsets.pipeline.stage.executor.shell.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;

import java.util.Collections;
import java.util.Map;

public class ShellConfig {

  // ENVIRONMENT tab

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    label = "Environment variables",
    description = "Variables that will be provided to the script as environment variables.",
    group = "ENVIRONMENT",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class},
    displayPosition = 10,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  public Map<String, String> environmentVariables = Collections.emptyMap();

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Timeout (ms)",
    defaultValue = "1000",
    description = "How long will the script be allowed to run. The time is in milliseconds.",
    displayPosition = 20,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {TimeEL.class},
    group = "ENVIRONMENT"
  )
  public String timeout = "1000";

  // SCRIPT tab

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.TEXT,
    label = "Script",
    group = "SCRIPT",
    mode = ConfigDef.Mode.SHELL,
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    displayPosition = 10,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String script;
}
