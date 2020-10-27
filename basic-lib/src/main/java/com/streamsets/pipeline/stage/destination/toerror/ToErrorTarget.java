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
package com.streamsets.pipeline.stage.destination.toerror;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class ToErrorTarget extends RecordTarget {
  private final boolean stopPipelineOnError;
  private final String errorMessage;
  private ELVars errorMessageVars;
  private ELEval errorMessageEval;

  public ToErrorTarget(boolean stopPipelineOnError, String errorMessage) {
    this.stopPipelineOnError = stopPipelineOnError;
    this.errorMessage = errorMessage;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorMessageVars = getContext().createELVars();
    errorMessageEval = getContext().createELEval("errorMessage");
    return issues;
  }

  /**
   * Overwriting write(Batch batch) method to handle OnRecordErrorException differently from RecordTarget.write method.
   */
  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> it = batch.getRecords();
    if (it.hasNext()) {
      while (it.hasNext()) {
        write(it.next());
      }
    }
  }

  @Override
  protected void write(Record record) {
    getContext().toError(record, Errors.TOERROR_00);
    if (stopPipelineOnError) {
      throw new OnRecordErrorException(Errors.TOERROR_01, getResolvedErrorMessage(errorMessage, record));
    }
  }

  public String getResolvedErrorMessage(String errorMessage, Record record) throws ELEvalException {
    RecordEL.setRecordInContext(errorMessageVars, record);
    TimeEL.setCalendarInContext(errorMessageVars, Calendar.getInstance());
    TimeNowEL.setTimeNowInContext(errorMessageVars, new Date());
    return errorMessageEval.eval(errorMessageVars, errorMessage, String.class);
  }
}
