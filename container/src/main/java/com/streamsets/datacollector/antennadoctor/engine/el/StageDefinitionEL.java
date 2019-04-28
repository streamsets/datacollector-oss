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

import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.pipeline.SDCClassLoader;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StageDefinitionEL {
  private static final String CONTEXT_VAR = "__content_stageDefinition";

  public static void setVars(ELVars variables, StageDefinition definition) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(CONTEXT_VAR, definition);
  }

  private static StageDefinition getStageDefinition() {
    return (StageDefinition) ELEval.getVariablesInScope().getContextVariable(CONTEXT_VAR);
  }

  @ElFunction(prefix = "stageDef", name = "type")
  public static String getType() {
    return getStageDefinition().getType().name();
  }

  @ElFunction(prefix = "stageDef", name = "classpath")
  public static List<String> classpath() {
    SDCClassLoader classLoader = (SDCClassLoader) getStageDefinition().getStageClassLoader();
    return Arrays.stream(classLoader.getURLs()).map(URL::getFile).collect(Collectors.toList());
  }
}
