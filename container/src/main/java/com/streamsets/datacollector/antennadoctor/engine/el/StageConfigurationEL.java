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

import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;

public class StageConfigurationEL {
  private static final String CONTEXT_VAR = "__content_stageConfiguration";

  public static void setVars(ELVars variables, StageConfiguration configuration) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(CONTEXT_VAR, configuration);
  }

  public static StageConfiguration getStageConfiguration() {
    return (StageConfiguration) ELEval.getVariablesInScope().getContextVariable(CONTEXT_VAR);
  }

  @ElFunction(prefix = "stageConf", name = "config")
  public static String getType(@ElParam("name") String name) {
    Config config = getStageConfiguration().getConfig(name);
    if(config == null) {
      return null;
    }

    return config.getValue() == null ? null : config.getValue().toString();
  }
}
