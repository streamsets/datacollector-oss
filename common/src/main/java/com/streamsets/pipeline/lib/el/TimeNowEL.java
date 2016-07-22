/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Date;

public class TimeNowEL {

  public static final String TIME_CONTEXT_VAR = "time";
  public static final String TIME_NOW_CONTEXT_VAR = "time_now";

  private TimeNowEL() {}

  @ElFunction(prefix = TIME_CONTEXT_VAR, name = "now", description = "")
  public static Date getTimeNowFunc() {
    Date now = (Date) ELEval.getVariablesInScope().getContextVariable(TIME_NOW_CONTEXT_VAR);
    if(null == now) {
      now = new Date();
    }
    return now;
  }

  @ElFunction(prefix = TIME_CONTEXT_VAR, name = "trimDate", description = "Set date portion of datetime expression to January 1, 1970")
  public static Date trimDate(@ElParam("datetime") Date in) {
    if(in == null) {
      return null;
    }

    Date ret = new Date(in.getTime());
    ret.setYear(70);
    ret.setMonth(0);
    ret.setDate(1);
    return ret;
  }

  @ElFunction(prefix = TIME_CONTEXT_VAR, name = "trimTime", description = "Set time portion of datetime expression to 00:00:00")
  public static Date trimTime(@ElParam("datetime") Date in) {
    if(in == null) {
      return null;
    }

    Date ret = new Date(in.getTime());
    ret.setHours(0);
    ret.setMinutes(0);
    ret.setSeconds(0);
    return ret;
  }

  public static void setTimeNowInContext(ELVars variables, Date now) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(TIME_NOW_CONTEXT_VAR, now);
  }
}
