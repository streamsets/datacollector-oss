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

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class TimeNowEL {
  private static final Logger LOG = LoggerFactory.getLogger(TimeNowEL.class);

  public static final String TIME_CONTEXT_VAR = "time";
  public static final String TIME_NOW_CONTEXT_VAR = "time_now";

  private TimeNowEL() {}

  @ElFunction(prefix = TIME_CONTEXT_VAR, name = "now", description = "Creates a Datetime object set to the current " +
      "time.")
  public static Date getTimeNowFunc() {
    Date now = (Date) ELEval.getVariablesInScope().getContextVariable(TIME_NOW_CONTEXT_VAR);
    if(null == now) {
      now = new Date();
    }
    return now;
  }

  @ElFunction(prefix = TIME_CONTEXT_VAR, name = "trimDate", description = "Set date portion of datetime expression to January 1, 1970")
  @SuppressWarnings("deprecation")
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
  @SuppressWarnings("deprecation")
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

  @ElFunction(prefix = TIME_CONTEXT_VAR,
      name = "millisecondsToDateTime",
      description = "Convert epoch in milliseconds to DateTime")
  public static Date millisecondsToDateTime(@ElParam("long") long in) {
    return new Date(in);

  }

  @ElFunction(prefix = TIME_CONTEXT_VAR,
      name = "dateTimeToMilliseconds",
      description = "Convert DateTime to epoch in milliseconds")
  public static long dateTimeToMilliseconds(@ElParam("datetime") Date in) {
    if (in == null) {
      return 0;
    }
    return in.getTime();
  }

  @ElFunction(prefix = TIME_CONTEXT_VAR,
      name = "extractStringFromDate",
      description = "Format a date into a string, based on an output format specification")
  public static String millisecondsToStringDate(@ElParam("datetime") Date in, @ElParam("string") String outputFormat) {
    if(in == null || outputFormat == null || outputFormat.isEmpty()) {
      return "";
    }

    SimpleDateFormat formatter = new SimpleDateFormat(outputFormat);
    return formatter.format(in);
  }

  @ElFunction(prefix = TIME_CONTEXT_VAR,
      name = "extractLongFromDate",
      description = "Format a date into a long, based on an output format specification")
  public static long milliSecondsToLongDate(@ElParam("datetime") Date in, @ElParam("string") String outputFormat)
      throws NumberFormatException {
    if(in == null || outputFormat == null || outputFormat.isEmpty()) {
      return 0;
    }

    SimpleDateFormat formatter = new SimpleDateFormat(outputFormat);
    String str = formatter.format(in);

    String value = str.replaceAll("[^0-9]","");
    return Long.parseLong(value);
  }

  @ElFunction(prefix = TIME_CONTEXT_VAR, name = "extractDateFromString", description = "Format a String date into a date.")
  public static Date extractDateFromString(
      @ElParam("dateTimeString") String dateTimeString,
      @ElParam("dateFormat") String dateFormat
  ) throws ParseException{
    if (StringUtils.isEmpty(dateTimeString)) {
      LOG.error(Utils.format("Invalid parameter - Date String is null/empty"));
      return null;
    }
    if (StringUtils.isEmpty(dateFormat)) {
      LOG.error(Utils.format("Invalid parameter - Date Format is null/empty"));
      return null;
    }
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat, Locale.US);
    return simpleDateFormat.parse(dateTimeString);
  }

  @ElFunction(prefix = TIME_CONTEXT_VAR, name = "extractStringFromDateTZ", description = "Format a Date into a " +
      "string" + " date, adjusting for time zone.")
  public static String extractStringFromDateTZ(
      @ElParam("datetime") Date in, @ElParam("timezone") String timeZone, @ElParam("dateFormat") String outputFormat
  ) {
    if (in == null) {
      LOG.error(Utils.format("Invalid parameter - Date is null"));
      return "";
    }

    if (StringUtils.isEmpty(outputFormat) || StringUtils.isEmpty(timeZone)) {
      LOG.error(Utils.format("Invalid parameter - outputFormat or timeZone"));
      return "";
    }

    TimeZone tz = TimeZone.getTimeZone(timeZone);

    // TimeZone.getTimeZone() returns "GMT" by default
    // when the time zone is not valid.
    //
    // so we must check if the user asked for "GMT".  otherwise,
    // if TimeZone.getTimeZone() returned GMT, the input time zone is not valid.
    //
    if ("GMT".equals(tz.getID()) && (!"GMT".equals(timeZone))) {
      LOG.error(Utils.format("Error matching time zone/invalid timezone. '{}' returning empty ", timeZone));
      return "";
    }

    SimpleDateFormat formatter;
    try {
      formatter = new SimpleDateFormat(outputFormat);
    } catch (IllegalArgumentException ex) {
      LOG.error(Utils.format("SimpleDateFormatter error.  Invalid outputFormat '{}' or timezone '{}'",
          outputFormat,
          timeZone,
          ex
      ));
      return "";
    }

    formatter.setTimeZone(tz);
    return formatter.format(in);
  }
  @ElFunction(prefix = TIME_CONTEXT_VAR,
      name = "createDateFromStringTZ",
      description = "Change String Date / append Timezone to a datetime object")
  public static Date createDateFromStringTZ(
      @ElParam("Date as String") String inDate,
      @ElParam("TimeZone") String timeZone,
      @ElParam("Date String's Format") String inFormat
  ) {

    if (StringUtils.isEmpty(inFormat) || StringUtils.isEmpty(timeZone) || StringUtils.isEmpty(inDate)) {
      return null;
    }

    TimeZone tz = TimeZone.getTimeZone(timeZone);
    if ("GMT".equals(tz.getID()) && (!"GMT".equals(timeZone))) {
      LOG.error(Utils.format("Invalid timezone. '{}'", timeZone));
      return null;
    }

    try {
      SimpleDateFormat formatter = new SimpleDateFormat(inFormat);
      formatter.setTimeZone(tz);
      return (formatter.parse(inDate));

    } catch (IllegalArgumentException | ParseException ex) {
      LOG.error(Utils.format("Date formatting error.  Invalid date '{}', timezone '{}' or format '{}'",
          inDate,
          timeZone,
          inFormat,
          ex
      ));
    }
    return null;
  }

}
