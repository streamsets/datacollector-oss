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
package com.streamsets.datacollector.rules;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.el.ELEval;

public class AlertInfoEL {

  private static final String ALERT_INFO = "ALERT_INFO";

  private AlertInfoEL() {}

  public static void setInfo(String msg) {
    if (ELEval.getVariablesInScope() != null) {
      ELEval.getVariablesInScope().addContextVariable(ALERT_INFO, msg);
    }
  }

  @ElFunction(
      prefix = "alert",
      name = "info",
      description = "Information about the Drift Data rule that triggered the alert"
  )
  @SuppressWarnings("unchecked")
  public static String info() {
    return (String) ELEval.getVariablesInScope().getContextVariable(ALERT_INFO);
  }

}
