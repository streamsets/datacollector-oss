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

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;

import java.util.ArrayList;
import java.util.List;

public class OffsetColumnEL {
  public static final String OFFSET_COLUMN_EL_PREFIX = "offset";

  private static final String TABLE_CONTEXT_VAR = "tableContext";

  private static TableContext getTableInContext() {
    return (TableContext) ELEval.getVariablesInScope().getContextVariable(TABLE_CONTEXT_VAR);
  }

  public static void setTableInContext(ELVars elVars, TableRuntimeContext tableRuntimeInContext) {
    Utils.checkNotNull(elVars, "elVars");
    elVars.addContextVariable(TABLE_CONTEXT_VAR, tableRuntimeInContext.getSourceTableContext());
  }

  @ElFunction(
      prefix = OFFSET_COLUMN_EL_PREFIX,
      name = "column",
      description = "Returns the positioned Offset Column for the current table")
  public static String getOffsetColumn(@ElParam("fieldPath") int position) {
    TableContext tableContext = getTableInContext();
    Utils.checkNotNull(tableContext, "No Table in the context");
    List<String> offsetColumns = new ArrayList<>(tableContext.getOffsetColumns());
    Utils.checkArgument(
        (position >= 0 && position < offsetColumns.size()),
        Utils.format(
            "Position {} not in the range of offset column list : [0, {}) for table: {}",
            position,
            offsetColumns.size(),
            tableContext.getQualifiedName()
        )
    );
    return offsetColumns.get(position);
  }

}
