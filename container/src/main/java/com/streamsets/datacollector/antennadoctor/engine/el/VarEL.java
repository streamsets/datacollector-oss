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
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Map;

public class VarEL {
  private static final String CONTEXT_VAR = "__content_vars";

  public static void resetVars(ELVars variables) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(CONTEXT_VAR, new HashMap<String, Object>());
  }

  private static Map<String, Object> getMap() {
    return (Map<String, Object>) ELEval.getVariablesInScope().getContextVariable(CONTEXT_VAR);
  }

  @ElFunction(prefix = "var", name = "set")
  public static boolean set(@ElParam("name") String name, @ElParam("value") Object value) {
    getMap().put(name, value);
    return true;
  }

  @ElFunction(prefix = "var", name = "get")
  public static Object get(@ElParam("name") String name) {
    return getMap().get(name);
  }
}
