/*
 * Copyright 2019 StreamSets Inc.
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

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.EventDef;
import com.streamsets.pipeline.api.EventFieldDef;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.lib.event.EventCreator;

@EventDef(
    type = TableFinishedEvent.TABLE_FINISHED_TAG,
    description = "Generated when the origin completes processing all data within a table.",
    version = TableFinishedEvent.VERSION
)
public class TableFinishedEvent {
    public static final String TABLE_FINISHED_TAG = "table-finished";
    public static final int VERSION = 1;

    @EventFieldDef(
        name = TableFinishedEvent.SCHEMA_FIELD,
        description = "The schema associated with the table."
    )
    public static final String SCHEMA_FIELD = "schema";

    @EventFieldDef(
        name = TableFinishedEvent.TABLE_FIELD,
        description = "The table that has been processed."
    )
    public static final String TABLE_FIELD = "table";

    public static final EventCreator EVENT_CREATOR = new EventCreator.Builder(TABLE_FINISHED_TAG, VERSION)
        .withRequiredField(SCHEMA_FIELD)
        .withRequiredField(TABLE_FIELD)
        .build();

    public static void createTableFinishedEvent(
        PushSource.Context context,
        BatchContext batchContext,
        TableRuntimeContext tableRuntimeContext
    ) {
        EVENT_CREATOR.create(context, batchContext)
            .with(SCHEMA_FIELD,  tableRuntimeContext.getSourceTableContext().getSchema())
            .with(TABLE_FIELD, tableRuntimeContext.getSourceTableContext().getTableName())
            .createAndSend();
    }
}



