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
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.hdfs.common.Errors;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class PathResolver {
  private static final String VALIDATE_CONTEXT = "validateContext";
  private static final String DATE_CONTEXT = "dateContext";
  private static final String TIME_UNIT = "timeUnit";
  private static final String TIME_INCREMENT_VALUE = "timeIncrement";
  private static final String[] FUNCTION_NAMES = { "YYYY() or YY()", "MM()", "DD()", "hh()", "mm()", "ss()"};

  private static final int[] UNITS_ORDERED = {
    Calendar.YEAR,
    Calendar.MONTH,
    Calendar.DAY_OF_MONTH,
    Calendar.HOUR_OF_DAY,
    Calendar.MINUTE,
    Calendar.SECOND
  };

  private static final Map<String, Integer> VALID_UNITS = ImmutableMap.<String, Integer>builder()
    //.put("YYYY", 3000)
    //.put("MM", 12)
    //.put("DD", 31)
    .put("hh", 23)
    .put("mm", 59)
    .put("ss", 59)
    .build();

  private static final Map<String, Integer> UNIT_TO_CALENDAR = ImmutableMap.<String, Integer>builder()
    .put("YYYY", Calendar.YEAR)
    .put("MM", Calendar.MONTH)
    .put("DD", Calendar.DAY_OF_MONTH)
    .put("hh", Calendar.HOUR_OF_DAY)
    .put("mm", Calendar.MINUTE)
    .put("ss", Calendar.SECOND)
    .build();

  private final Stage.Context context;
  private final String pathTemplate;
  private int incrementUnit;
  private int incrementValue;
  private final TimeZone timeZone;
  private final ELVars elVars;
  private final ELEval freqEdgeElEval;
  private final ELEval pathEval;
  private boolean validated;

  public PathResolver(Stage.Context context, String config, String pathTemplate, TimeZone timeZone) {
    this.context = context;
    this.pathTemplate = pathTemplate;
    this.timeZone = timeZone;
    elVars = context.createELVars();
    freqEdgeElEval = context.createELEval(config, FrequencyEdgeEL.class);
    pathEval = context.createELEval(config);
  }

  public static class ValidateEL {

    private ValidateEL() {}

    private static int[] getFunctionsUsage() {
      return(int[]) ELEval.getVariablesInScope().getContextVariable(VALIDATE_CONTEXT);
    }

    @ElFunction(name = "YYYY")
    public static String YYYY() {
      getFunctionsUsage()[0] += 1;
      getFunctionsUsage()[11] = Math.max(getFunctionsUsage()[11], Calendar.YEAR);
      return "YYYY";
    }

    @ElFunction(name = "YY")
    public static String YY() {
      return YYYY();
    }

    @ElFunction(name = "MM")
    public static String MM() {
      getFunctionsUsage()[1] += 1;
      getFunctionsUsage()[11] = Math.max(getFunctionsUsage()[11], Calendar.MONTH);
      return "MM";
    }

    @ElFunction(name = "DD")
    public static String DD() {
      getFunctionsUsage()[2]+= 1;
      getFunctionsUsage()[11] = Math.max(getFunctionsUsage()[11], Calendar.DAY_OF_MONTH);
      return "DD";
    }

    @ElFunction(name = "hh")
    public static String hh() {
      getFunctionsUsage()[3] += 1;
      getFunctionsUsage()[11] = Math.max(getFunctionsUsage()[11], Calendar.HOUR_OF_DAY);
      return "hh";
    }

    @ElFunction(name = "mm")
    public static String mm() {
      getFunctionsUsage()[4] += 1;
      getFunctionsUsage()[11] = Math.max(getFunctionsUsage()[11], Calendar.MINUTE);
      return "mm";
    }

    @ElFunction(name = "ss")
    public static String ss() {
      getFunctionsUsage()[5] += 1;
      getFunctionsUsage()[11] = Math.max(getFunctionsUsage()[11], Calendar.SECOND);
      return "ss";
    }

    @ElFunction(name = "every")
    public static String every(@ElParam("value") int value, @ElParam("unit") String unit) {
      getFunctionsUsage()[6] = getFunctionsUsage()[6] + 1;
      if (!VALID_UNITS.containsKey(unit)) {
        getFunctionsUsage()[7] = 1;
      } else if (value < 1 || value > VALID_UNITS.get(unit)) {
        getFunctionsUsage()[8] = VALID_UNITS.get(unit);
      } else {
        getFunctionsUsage()[9] = UNIT_TO_CALENDAR.get(unit);
        getFunctionsUsage()[10] = value;
      }
      return "";
    }
  }

  private static class DateContext {
    private final Calendar original;
    private final Calendar adjusted;
    private boolean noDate = true;
    private int frequency;
    private int frequencyUnit;

    public DateContext(Date date, TimeZone timeZone) {
      original = Calendar.getInstance(timeZone);
      original.setTime(date);
      adjusted = Calendar.getInstance(timeZone);
      adjusted.setTime(date);

      // we drop the millisecs as we don't use them
      adjusted.set(Calendar.MILLISECOND, 0);

      frequency = 1;
      frequencyUnit = 0;
    }

    public void revert(int unit) {
      adjusted.set(unit, original.get(unit));
    }
  }

  public static class FrequencyEdgeEL {

    private FrequencyEdgeEL() {}

    private static void revertUnit(int unit) {
      DateContext context = (DateContext)ELEval.getVariablesInScope().getContextVariable(DATE_CONTEXT);
      context.noDate = false;
      context.revert(unit);
    }

    @ElFunction(name = "YYYY")
    public static String YYYY() {
      ((DateContext)ELEval.getVariablesInScope().getContextVariable(DATE_CONTEXT)).noDate = false;
      return "YYYY";
    }

    @ElFunction(name = "YY")
    public static String YY() {
      return YYYY();
    }

    @ElFunction(name = "MM")
    public static String MM() {
      revertUnit(Calendar.MONTH);
      return "MM";
    }

    @ElFunction(name = "DD")
    public static String DD() {
      revertUnit(Calendar.DAY_OF_MONTH);
      return "DD";
    }

    @ElFunction(name = "hh")
    public static String hh() {
      revertUnit(Calendar.HOUR_OF_DAY);
      return "hh";
    }

    @ElFunction(name = "mm")
    public static String mm() {
      revertUnit(Calendar.MINUTE);
      return "mm";
    }

    @ElFunction(name = "ss")
    public static String ss() {
      revertUnit(Calendar.SECOND);
      return "ss";
    }

    @ElFunction(name = "every")
    public static String every(@ElParam("value") int value, @ElParam("unit") String unit) {
      DateContext context = (DateContext)ELEval.getVariablesInScope().getContextVariable(DATE_CONTEXT);
      context.noDate = false;
      context.frequency = value;
      context.frequencyUnit = UNIT_TO_CALENDAR.get(unit);
      return "";
    }

  }

  public boolean validate(String group, String config, String qualifiedConfigName, List<Stage.ConfigIssue> issues) {
    int previousIssuesCount = issues.size();
    int[] validationInfo = new int[12];
    // 0: YYYY() & YY() count
    // 1: MM() count
    // 2: DD() count
    // 3: hh() count
    // 4: mm() count
    // 5: ss() count
    // 6: every() count
    // 7: if > 0, unit in every() is invalid
    // 8: if > 0, value in every() is outside of domain
    // 9: every() unit
    // 10: every() value
    // 11: minimum unit in path
    ELVars elVars = context.createELVars();
    elVars.addContextVariable(VALIDATE_CONTEXT, validationInfo);
    ELEval elEval = context.createELEval(config, ValidateEL.class);
    try {
      RecordEL.setRecordInContext(elVars, context.createRecord("validateDirPathTemplate"));
      elEval.eval(elVars, pathTemplate, String.class);
      boolean consecutive = true;
      for (int i = 0; i < 6; i++) {
        if (validationInfo[i] == 0) {
          consecutive = false;
        } else if (!consecutive) {
          issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_29, FUNCTION_NAMES[i],
                                               FUNCTION_NAMES[i - 1]));
          break;
        }
      }
      if (validationInfo[6] > 1) {
        // the every() function is more than once in the path
        issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_30));
      }
      if (validationInfo[7] > 0) {
        // the unit specified in the every() function is invalid or it does not work with every()
        issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_31));
      }
      if (validationInfo[8] > 0) {
        // the value specified in the every() function is outside of the unit domain
        issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_32, validationInfo[8]));
      }
      if (validationInfo[9] > 0 && validationInfo[9] < validationInfo[11]) {
        // the unit specified in the every() function is not the smallest unit in the path
        issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_33, validationInfo[8]));
      } else if (validationInfo[9] > 0) {
        if ((validationInfo[9] == Calendar.SECOND || validationInfo[9] == Calendar.MINUTE) &&
            (60 % validationInfo[10] != 0)) {
          issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_34));
        }
        if (validationInfo[9] == Calendar.HOUR_OF_DAY && (24 % validationInfo[10] != 0)) {
          issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_34));
        }
        if (validationInfo[9] == Calendar.SECOND && validationInfo[5] > 1) {
          issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_36));
        }
        if (validationInfo[9] == Calendar.MINUTE && validationInfo[4] > 1) {
          issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_37));
        }
        if (validationInfo[9] == Calendar.HOUR_OF_DAY && validationInfo[3] > 1) {
          issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_38));
        }
      }
    } catch (ELEvalException ex) {
      issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_20, ex.toString()));
    }
    validated = (issues.size() - previousIssuesCount) == 0;
    if (validated) {
      try {
        incrementUnit = evaluateTimeIncrementUnit(config);
        incrementValue = evaluateTimeIncrementValue(config);
      } catch (ELEvalException ex) {
        issues.add(context.createConfigIssue(group, qualifiedConfigName, Errors.HADOOPFS_35, ex.toString()));
        validated = false;
      }
    }
    return validated;
  }

  public Date getFloorDate(Date date) {
    return getDate(date, true);
  }

  public Date getCeilingDate(Date date) {
    return getDate(date, false);
  }

  public Date getDate(Date date, boolean floorDate) {
    Utils.checkState(validated, Utils.formatL("PathTemplateEL for '{}' must be validated", pathTemplate));
    DateContext dc = new DateContext(date, timeZone);
    elVars.addContextVariable(DATE_CONTEXT, dc);
    try {
      freqEdgeElEval.eval(elVars, pathTemplate, String.class);
    } catch (ELEvalException ex) {
      throw new RuntimeException(Utils.format("Unexpected exception: {}", ex.toString()), ex);
    }
    elVars.addContextVariable(DATE_CONTEXT, null);
    if (!dc.noDate) {
      int rangeEdgeCorrection = floorDate ? 0 : 1;
      // set to the minimum all values for units smaller than the the incrementUnit
      for (int i = 0; i < UNITS_ORDERED.length; i++) {
        if (UNITS_ORDERED[i] > incrementUnit) {
          dc.adjusted.set(UNITS_ORDERED[i], dc.adjusted.getMinimum(UNITS_ORDERED[i]));
        }
      }
      // we don't do millis, still we need to set it to zero.
      dc.adjusted.set(Calendar.MILLISECOND, 0);

      if (dc.frequency > 1) {
        if (dc.frequencyUnit == 0) {
          throw new IllegalStateException(Utils.format(
              "Unknown error every() function in path template seems to have an invalid unit: {}",
              pathTemplate));
        }
        // because we have a frequency greater than 1, we need to adjust to the floor of the frequency range

        // compute the the floor/ceiling of the frequency range
        int frequencyRangeEdge = (dc.adjusted.get(dc.frequencyUnit) / dc.frequency + rangeEdgeCorrection) * dc.frequency;

        // set the value of the frequency unit to its minimum value
        dc.adjusted.set(dc.frequencyUnit, dc.adjusted.getMinimum(dc.frequencyUnit));

        // add the floor of the frequency root to get the proper floor/ceiling
        dc.adjusted.add(dc.frequencyUnit, frequencyRangeEdge);
      } else {
        dc.adjusted.add(incrementUnit, rangeEdgeCorrection);
      }
      if (!floorDate) {
        // in case of computing ceiling, the range is a [,) interval, subtracting one millisec ensures that.
        dc.adjusted.add(Calendar.MILLISECOND, -1);
      }
      date = dc.adjusted.getTime();
    } else {
      date = null;
    }
    return date;
  }

  String resolvePath(Date date, Record record) throws StageException {
    try {
      ELVars vars = context.createELVars();
      RecordEL.setRecordInContext(vars, record);
      date = getFloorDate(date);
      if (date != null) {
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.setTime(date);
        TimeEL.setCalendarInContext(vars, calendar);
      }
      return pathEval.eval(vars, pathTemplate, String.class);
    } catch (ELEvalException ex) {
      throw new StageException(Errors.HADOOPFS_02, pathTemplate, ex.toString(), ex);
    }
  }

  public static class TimeIncrementUnitEL {

    private TimeIncrementUnitEL() {}

    private static void adjustTimeUnit(int unit) {
      Integer currentUnit = (Integer) ELEval.getVariablesInScope().getContextVariable(TIME_UNIT);
      if (currentUnit == null || currentUnit < unit) {
        ELEval.getVariablesInScope().addVariable(TIME_UNIT, unit);
      }
    }

    @ElFunction(name = "YYYY")
    public static int YYYY() {
      adjustTimeUnit(Calendar.YEAR);
      return Calendar.YEAR;
    }

    @ElFunction(name = "YY")
    public static int YY() {
      return YYYY();
    }

    @ElFunction(name = "MM")
    public static int MM() {
      adjustTimeUnit(Calendar.MONTH);
      return Calendar.MONTH;
    }

    @ElFunction(name = "DD")
    public static int DD() {
      adjustTimeUnit(Calendar.DAY_OF_MONTH);
      return Calendar.DAY_OF_MONTH;
    }

    @ElFunction(name = "hh")
    public static int hh() {
      adjustTimeUnit(Calendar.HOUR_OF_DAY);
      return Calendar.HOUR_OF_DAY;
    }

    @ElFunction(name = "mm")
    public static int mm() {
      adjustTimeUnit(Calendar.MINUTE);
      return Calendar.MINUTE;
    }

    @ElFunction(name = "ss")
    public static int ss() {
      adjustTimeUnit(Calendar.SECOND);
      return Calendar.SECOND;
    }

    @ElFunction(name = "every")
    public static int every(@ElParam("value") int value, @ElParam("unit") int unit) {
      switch (unit){
        case Calendar.YEAR:
        case Calendar.MONTH:
        case Calendar.DAY_OF_MONTH:
        case Calendar.HOUR_OF_DAY:
        case Calendar.MINUTE:
        case Calendar.SECOND:
          ELEval.getVariablesInScope().getContextVariable(TIME_INCREMENT_VALUE);
          break;
        default:
          throw new IllegalArgumentException(Utils.format("Invalid Calendar unit ordinal '{}'", unit));
      }
      return 0;
    }

  }

  private int evaluateTimeIncrementUnit(String config) throws ELEvalException {
    ELVars elVars = context.createELVars();
    elVars.addVariable(TIME_UNIT, 0);
    ELEval elEval = context.createELEval(config, TimeIncrementUnitEL.class);
    RecordEL.setRecordInContext(elVars, context.createRecord("validateDirPathTemplate"));
    elEval.eval(elVars, pathTemplate, String.class);
    return (int) elVars.getVariable(TIME_UNIT);
  }

  private int evaluateTimeIncrementValue(String config) throws ELEvalException {
    ELVars elVars = context.createELVars();
    elVars.addVariable(TIME_INCREMENT_VALUE, 1);
    ELEval elEval = context.createELEval(config, TimeIncrementUnitEL.class);
    RecordEL.setRecordInContext(elVars, context.createRecord("validateDirPathTemplate"));
    elEval.eval(elVars, pathTemplate, String.class);
    return (int) elVars.getVariable(TIME_INCREMENT_VALUE);
  }

  public int getTimeIncrementUnit()  {
    return incrementUnit;
  }

  public int getTimeIncrementValue() {
    return incrementValue;
  }

}
