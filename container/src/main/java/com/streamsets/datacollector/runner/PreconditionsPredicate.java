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
package com.streamsets.datacollector.runner;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.creation.StageConfigBean;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;

import java.util.List;

public class PreconditionsPredicate implements FilterRecordBatch.Predicate  {
  private final List<String> preconditions;
  private final ELVars elVars;
  private final ELEval elEval;
  private String failedPrecondition;
  private Exception exception;

  public PreconditionsPredicate(Stage.Context context, List<String> preconditions) {
    this.preconditions = preconditions;
    elVars = context.createELVars();
    elEval = context.createELEval(StageConfigBean.STAGE_PRECONDITIONS_CONFIG, RecordEL.class, StringEL.class,
                                  RuntimeEL.class);
  }

  @Override
  public boolean evaluate(Record record) {
    failedPrecondition = null;
    exception = null;
    boolean eval = true;
    if (preconditions != null && !preconditions.isEmpty()) {
      RecordEL.setRecordInContext(elVars, record);
      for (String precondition : preconditions) {
        try {
          failedPrecondition = precondition;
          eval = elEval.eval(elVars, precondition, Boolean.class);
        } catch (ELEvalException ex) {
          eval = false;
          exception = ex;
        }
        if (!eval) {
          break;
        }
      }
    }
    if (eval) {
      failedPrecondition = null;
    }
    return eval;
  }

  @Override
  public ErrorMessage getRejectedMessage() {
    Preconditions.checkState(failedPrecondition != null || exception != null,
                             "Called for record that passed all preconditions");
    ErrorMessage msg = null;
    if (failedPrecondition != null) {
      msg = (exception == null) ? new ErrorMessage(ContainerError.CONTAINER_0051, failedPrecondition)
                                : new ErrorMessage(ContainerError.CONTAINER_0052, failedPrecondition,
                                                   exception.toString(), exception);
    }
    return msg;
  }

}
