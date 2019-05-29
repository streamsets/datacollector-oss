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

import java.util.List;

@EventDef(
    type = SchemaFinishedEvent.SCHEMA_FINISHED_TAG,
    description = "Generated when the origin completes processing all data within a schema.",
    version = SchemaFinishedEvent.VERSION
)
public class SchemaFinishedEvent {
    public static final String SCHEMA_FINISHED_TAG = "schema-finished";
    public static final int VERSION = 1;

    @EventFieldDef(
        name = SchemaFinishedEvent.SCHEMA_FIELD,
        description = "The schema that has been fully processed."
    )
    public static final String SCHEMA_FIELD = "schema";

    @EventFieldDef(
        name = SchemaFinishedEvent.TABLES_FIELD,
        description = "A list of the tables in the schema that have been fully processed."
    )
    public static final String TABLES_FIELD = "tables";

    public static final EventCreator EVENT_CREATOR = new EventCreator.Builder(SCHEMA_FINISHED_TAG, VERSION)
        .withRequiredField(SCHEMA_FIELD)
        .withRequiredField(TABLES_FIELD)
        .build();

    public static void createSchemaFinishedEvent(
        PushSource.Context context,
        BatchContext batchContext,
        TableRuntimeContext tableRuntimeContext,
        List<String> allTables
    ) {
        EVENT_CREATOR.create(context, batchContext)
            .with(SCHEMA_FIELD, tableRuntimeContext.getSourceTableContext().getSchema())
            .withStringList(TABLES_FIELD, allTables)
            .createAndSend();
    }
}



