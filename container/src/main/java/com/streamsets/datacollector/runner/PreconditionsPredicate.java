/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
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
    elEval = context.createELEval(ConfigDefinition.PRECONDITIONS, RecordEL.class, StringEL.class, RuntimeEL.class);
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
      msg = (exception == null) ? new ErrorMessage(ContainerError.CONTAINER_0051, preconditions)
                                : new ErrorMessage(ContainerError.CONTAINER_0052, preconditions,
                                                   exception.toString(), exception);
    }
    return msg;
  }

}
