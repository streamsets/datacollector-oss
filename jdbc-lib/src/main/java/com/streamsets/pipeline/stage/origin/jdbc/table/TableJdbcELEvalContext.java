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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;

import java.util.Calendar;
import java.util.Date;

public final class TableJdbcELEvalContext {
  private final ELVars elVars;
  private final Stage.Context context;

  public TableJdbcELEvalContext(Stage.Context context, ELVars elVars) {
    this.context = context;
    this.elVars = elVars;
  }

  public void setCalendar(Calendar calendar) {
    TimeEL.setCalendarInContext(elVars, calendar);
  }

  public void setTime(Date date) {
    TimeNowEL.setTimeNowInContext(elVars, date);
  }

  public void setTableContext(TableRuntimeContext tableContext) {
    OffsetColumnEL.setTableInContext(elVars, tableContext);
  }

  public String evaluateAsString(String name, String expression) throws ELEvalException {
    return createELEval(name).eval(elVars, expression, String.class);
  }

  private ELEval createELEval(String name) {
    return context.createELEval(name);
  }
}
