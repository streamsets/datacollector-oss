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
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class PreconditionsPredicate implements FilterRecordBatch.Predicate  {
  private final List<String> preconditions;
  private final ELVars elVars;
  private final ELEval elEval;
  private List<String> failedPreconditions;
  private Exception exception;

  public PreconditionsPredicate(Stage.Context context, List<String> preconditions) {
    this.preconditions = preconditions;
    this.failedPreconditions = new ArrayList<>();
    elVars = context.createELVars();
    elEval = context.createELEval(
        StageConfigBean.STAGE_PRECONDITIONS_CONFIG,
        RecordEL.class,
        StringEL.class,
        RuntimeEL.class
    );
  }

  @Override
  public boolean evaluate(Record record) {
    exception = null;
    failedPreconditions.clear();

    if (preconditions != null && !preconditions.isEmpty()) {
      RecordEL.setRecordInContext(elVars, record);
      for (String precondition : preconditions) {
        try {
          if(!elEval.eval(elVars, precondition, Boolean.class)) {
            failedPreconditions.add(precondition);
          }
        } catch (ELEvalException ex) {
          // We hit error while evaluating, store exception and the precondition that generated the error
          exception = ex;
          failedPreconditions.add(precondition);
        }
      }
    }

    return failedPreconditions.isEmpty();
  }

  @Override
  public ErrorMessage getRejectedMessage() {
    Preconditions.checkState(!failedPreconditions.isEmpty(), "Called for record that passed all preconditions");
    String failures = StringUtils.join(failedPreconditions, ",");

    if(exception == null) {
      return new ErrorMessage(ContainerError.CONTAINER_0051, failures, ",");
    } else {
      return new ErrorMessage(ContainerError.CONTAINER_0052, failures, exception);
    }
  }

}
