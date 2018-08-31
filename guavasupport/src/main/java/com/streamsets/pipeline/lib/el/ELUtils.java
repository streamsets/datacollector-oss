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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Note: This class would make more sense in stagesupport module, but it does expose guava in it's public contact
 * and hence can't be there. Perhaps in the future we might explore feasibility of removing  the guava dependency and
 * subsequently moving this code to stagesupport.
 */
public class ELUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ELUtils.class);

  private ELUtils() {}

  public static ELVars parseConstants(Map<String,?> constants, Stage.Context context, String group,
      String config, ErrorCode err, List<Stage.ConfigIssue> issues) {
    ELVars variables = context.createELVars();
    if (constants != null) {
      for (Map.Entry<String, ?> entry : constants.entrySet()) {
        try {
          variables.addVariable(entry.getKey(), entry.getValue());
        } catch (Exception ex) {
          issues.add(context.createConfigIssue(group, config, err, constants, ex.toString(), ex));
        }
      }
    }
    return variables;
  }

  @Deprecated
  public static void validateExpression(
      ELEval elEvaluator,
      ELVars variables,
      String expression,
      Stage.Context context,
      String group,
      String config,
      ErrorCode err,
      Class<?> type,
      List<Stage.ConfigIssue> issues
  ) {
    validateExpression(expression, context, group, config, err, issues);
    try {
      context.parseEL(expression);
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(group, config, err, expression, ex.toString(), ex));
    }
  }

  public static void validateExpression(
      String expression,
      Stage.Context context,
      String group,
      String config,
      ErrorCode err,
      List<Stage.ConfigIssue> issues
  )
  {
    try {
      context.parseEL(expression);
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(group, config, err, expression, ex.toString(), ex));
    }
  }

  public static Multimap<String, Record> partitionBatchByExpression(
      ELEval elEvaluator,
      ELVars variables,
      String expression,
      Batch batch
  ) throws OnRecordErrorException {
    return partitionBatchByExpression(
        elEvaluator,
        variables,
        expression,
        null,
        null,
        null,
        null,
        batch
    );
  }

  public static Multimap<String, Record> partitionBatchByExpression(
      ELEval elEvaluator,
      ELVars variables,
      String expression,
      ELEval timeDriverEvaluator,
      ELVars timeDriverVariables,
      String timeDriverExpression,
      Calendar calendar,
      Batch batch
  ) throws OnRecordErrorException {
    Multimap<String, Record> partitions = ArrayListMultimap.create();

    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      RecordEL.setRecordInContext(variables, record);
      // The expression may not include time EL in which case time driver parameters will be null.
      if (timeDriverEvaluator != null) {
        Date recordTime = getRecordTime(
            timeDriverEvaluator,
            timeDriverVariables,
            timeDriverExpression,
            record
        );
        calendar.setTime(recordTime);
        TimeEL.setCalendarInContext(variables, calendar);
        TimeNowEL.setTimeNowInContext(variables, recordTime);
      }

      try {
        String partitionName = elEvaluator.eval(variables, expression, String.class);
        LOG.debug("Expression '{}' is evaluated to '{}' : ", expression, partitionName);
        partitions.put(partitionName, record);
      } catch (ELEvalException e) {
        LOG.error("Failed to evaluate expression '{}' : ", expression, e.toString(), e);
        throw new OnRecordErrorException(record, e.getErrorCode(), e.getParams());
      }
    }

    return partitions;
  }

  public static Date getRecordTime(
      ELEval elEvaluator,
      ELVars variables,
      String expression,
      Record record
  ) throws OnRecordErrorException {
    try {
      TimeNowEL.setTimeNowInContext(variables, new Date());
      RecordEL.setRecordInContext(variables, record);
      return elEvaluator.eval(variables, expression, Date.class);
    } catch (ELEvalException e) {
      LOG.error("Failed to evaluate expression '{}' : ", expression, e.toString(), e);
      throw new OnRecordErrorException(record, e.getErrorCode(), e.getParams());
    }
  }
}
