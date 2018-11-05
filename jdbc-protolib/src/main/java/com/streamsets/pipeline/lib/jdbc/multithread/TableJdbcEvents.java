/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.lib.jdbc.multithread;

import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.lib.event.EventCreator;

import java.util.List;

public abstract class TableJdbcEvents {
  private TableJdbcEvents() {
  }

  public static final String TABLE_FINISHED_TAG = "table-finished";
  public static final String TABLE_FIELD = "table";
  public static final String SCHEMA_FIELD = "schema";
  public static final EventCreator TABLE_FINISHED = new EventCreator.Builder(TABLE_FINISHED_TAG, 1)
      .withRequiredField(SCHEMA_FIELD)
      .withRequiredField(TABLE_FIELD)
      .build();

  public static final String SCHEMA_FINISHED_TAG = "schema-finished";
  public static final String TABLES_FIELD = "tables";
  public static final EventCreator SCHEMA_FINISHED = new EventCreator.Builder(SCHEMA_FINISHED_TAG, 1)
      .withRequiredField(SCHEMA_FIELD)
      .withRequiredField(TABLES_FIELD)
      .build();

  public static void createTableFinishedEvent(
      PushSource.Context context,
      TableRuntimeContext tableRuntimeContext
  ) {
    TABLE_FINISHED.create(context, context.startBatch()).with(
        SCHEMA_FIELD,
        tableRuntimeContext.getSourceTableContext().getSchema()
    ).with(
        TABLE_FIELD,
        tableRuntimeContext.getSourceTableContext().getTableName()
    ).createAndSend();
  }

  public static void createSchemaFinishedEvent(
      PushSource.Context context,
      TableRuntimeContext tableRuntimeContext,
      List<String> allTables
  ) {
    SCHEMA_FINISHED.create(context, context.startBatch()).with(
        SCHEMA_FIELD,
        tableRuntimeContext.getSourceTableContext().getSchema()
    ).withStringList(TABLES_FIELD, allTables).createAndSend();
  }
}
