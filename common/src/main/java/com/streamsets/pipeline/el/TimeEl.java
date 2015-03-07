/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Calendar;
import java.util.Date;

public class TimeEl {

  public static final String TIME_CONTEXT_VAR = "time";
  public static final String TIME_NOW_CONTEXT_VAR = "time_now";
  public static final String CALENDER_CONTEXT_VAR = "calender";

  @ElConstant(prefix = "", name = "HOURS", description = "")
  public static final int HOURS = 60*60;

  @ElConstant(prefix = "", name = "MINUTES", description = "")
  public static final int MINUTES = 60;

  @ElFunction(prefix = TIME_CONTEXT_VAR, name = "now", description = "")
  public static Date getTimeNowFunc() {
    Date now = (Date) ELEval.getVariablesInScope().getContextVariable(TIME_NOW_CONTEXT_VAR);
    Utils.checkArgument(now != null, "time:now() function has not been properly initialized");
    return now;
  }

  public static void setTimeNowInContext(ELEval.Variables variables, Date now) {
    Utils.checkNotNull(variables, "variables");
    variables.addContextVariable(TIME_NOW_CONTEXT_VAR, now);
  }

  public static void setCalendarInContext(ELEval.Variables variables, Calendar calendar) {
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

}
