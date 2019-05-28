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
package com.streamsets.pipeline.stage.bigquery.origin;

import com.streamsets.pipeline.api.EventDef;
import com.streamsets.pipeline.api.EventFieldDef;
import com.streamsets.pipeline.lib.event.EventCreator;

@EventDef(
    type = BigQuerySuccessEvent.BIG_QUERY_SUCCESS_TAG,
    description = "Generated when the origin successfully completes a query.",
    version = BigQuerySuccessEvent.VERSION
)
public class BigQuerySuccessEvent {
    public static final String BIG_QUERY_SUCCESS_TAG = "big-query-success";
    public static final int VERSION = 1;

    @EventFieldDef(
        name = BigQuerySuccessEvent.QUERY,
        description = "Query that completed successfully.",
        optional = true
    )
    public static final String QUERY = "query";

    @EventFieldDef(
        name = BigQuerySuccessEvent.TIMESTAMP,
        description = "Timestamp when the query completed.",
        optional = true
    )
    public static final String TIMESTAMP = "timestamp";

    @EventFieldDef(
        name = BigQuerySuccessEvent.ROW_COUNT,
        description = "Number of processed rows.",
        optional = true
    )
    public static final String ROW_COUNT = "rows";

    @EventFieldDef(
        name = BigQuerySuccessEvent.SOURCE_OFFSET,
        description = "Offset after the query completed.",
        optional = true
    )
    public static final String SOURCE_OFFSET = "offset";

    public static final EventCreator EVENT_CREATOR = new EventCreator.Builder("big-query-success", 1)
        .withRequiredField(QUERY)
        .withRequiredField(TIMESTAMP)
        .withRequiredField(ROW_COUNT)
        .withRequiredField(SOURCE_OFFSET)
        .build();
}



