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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

  public static void validateExpression(ELEval elEvaluator, ELVars variables, String expression,
      Stage.Context context, String group, String config, ErrorCode err, Class<?> type, List<Stage.ConfigIssue> issues)
  {
    RecordEL.setRecordInContext(variables, context.createRecord("forValidation"));
    try {
      context.parseEL(expression);
      elEvaluator.eval(variables, expression, type);
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(group, config, err, expression, ex.toString(), ex));
    }
  }

  public static Multimap<String, Record> partitionBatchByExpression(
      ELEval elEvaluator,
      ELVars variables,
      String expression,
      Batch batch
  ) {
    Multimap<String, Record> partitions = ArrayListMultimap.create();

    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      RecordEL.setRecordInContext(variables, record);
      try {
        String partitionName = elEvaluator.eval(variables, expression, String.class);
        LOG.debug("Expression '{}' is evaluated to '{}' : ", expression, partitionName);
        partitions.put(partitionName, record);
      } catch (ELEvalException e) {
        LOG.error("Failed to evaluate expression '{}' : ", expression, e.toString(), e);
      }
    }

    return partitions;
  }

}
