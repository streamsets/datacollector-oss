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
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Calendar;

public class TimeEL {

  public static final String CALENDER_CONTEXT_VAR = "calender";

  @ElConstant(name = "HOURS", description = "")
  public static final int HOURS = 60*60;

  @ElConstant(name = "MINUTES", description = "")
  public static final int MINUTES = 60;

  @ElConstant(name = "SECONDS", description = "")
  public static final int SECONDS = 1;

  private TimeEL() {}

  public static void setCalendarInContext(ELVars variables, Calendar calendar) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(CALENDER_CONTEXT_VAR, calendar);
  }

  @ElFunction(prefix = "", name = "YYYY", description = "")
  public static String getYear() {
    Calendar calendar = (Calendar) ELEval.getVariablesInScope().getContextVariable(CALENDER_CONTEXT_VAR);
    return Utils.intToPaddedString(calendar.get(Calendar.YEAR), 4);
  }

  @ElFunction(prefix = "", name = "YY", description = "")
  public static String getShortYear() {
    Calendar calendar = (Calendar) ELEval.getVariablesInScope().getContextVariable(CALENDER_CONTEXT_VAR);
    String year = Utils.intToPaddedString(calendar.get(Calendar.YEAR), 4);
    return year.substring(year.length() - 2);
  }

  @ElFunction(prefix = "", name = "MM", description = "")
  public static String getMonth() {
    Calendar calendar = (Calendar) ELEval.getVariablesInScope().getContextVariable(CALENDER_CONTEXT_VAR);
    return Utils.intToPaddedString(calendar.get(Calendar.MONTH) + 1, 2);
  }

  @ElFunction(prefix = "", name = "DD", description = "")
  public static String getDay() {
    Calendar calendar = (Calendar) ELEval.getVariablesInScope().getContextVariable(CALENDER_CONTEXT_VAR);
    return Utils.intToPaddedString(calendar.get(Calendar.DAY_OF_MONTH), 2);
  }

  @ElFunction(prefix = "", name = "hh", description = "")
  public static String getHour() {
    Calendar calendar = (Calendar) ELEval.getVariablesInScope().getContextVariable(CALENDER_CONTEXT_VAR);
    return Utils.intToPaddedString(calendar.get(Calendar.HOUR_OF_DAY), 2);
  }

  @ElFunction(prefix = "", name = "mm", description = "")
  public static String getMinute() {
    Calendar calendar = (Calendar) ELEval.getVariablesInScope().getContextVariable(CALENDER_CONTEXT_VAR);
    return Utils.intToPaddedString(calendar.get(Calendar.MINUTE), 2);
  }

  @ElFunction(prefix = "", name = "ss", description = "")
  public static String getSecond() {
    Calendar calendar = (Calendar) ELEval.getVariablesInScope().getContextVariable(CALENDER_CONTEXT_VAR);
    return Utils.intToPaddedString(calendar.get(Calendar.SECOND), 2);
  }

  @ElFunction(prefix = "", name = "SSS", description = "")
  public static String getMilisecond() {
    Calendar calendar = (Calendar) ELEval.getVariablesInScope().getContextVariable(CALENDER_CONTEXT_VAR);
    return Utils.intToPaddedString(calendar.get(Calendar.MILLISECOND), 3);
  }

}
