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
package com.streamsets.pipeline.lib.event;

import com.streamsets.pipeline.api.EventDef;
import com.streamsets.pipeline.api.EventFieldDef;

@EventDef(
    type = NoMoreDataEvent.NO_MORE_DATA_TAG,
    description = "Generated after the origin processes all data.",
    version = NoMoreDataEvent.VERSION
)
public class NoMoreDataEvent {
    public static final String NO_MORE_DATA_TAG = "no-more-data";
    public static final int VERSION = 2;

    @EventFieldDef(
        name = NoMoreDataEvent.RECORD_COUNT,
        description = "Number of records successfully processed since the pipeline started or since the last " +
            "no-more-data event was generated.",
        optional = true
    )
    public static final String RECORD_COUNT = "record-count";

    @EventFieldDef(
        name = NoMoreDataEvent.ERROR_COUNT,
        description = "Number of error records generated since the pipeline started or since the last no-more-data " +
            "event was created.",
        optional = true
    )
    public static final String ERROR_COUNT = "error-count";

    @EventFieldDef(
        name = NoMoreDataEvent.FILE_COUNT,
        description = "Number of objects that the origin successfully processed and attempted to process. " +
            "Includes objects that could not be processed or fully processed, as well as successfully processed objects.",
        optional = true
    )
    public static final String FILE_COUNT = "file-count";

    public static EventCreator EVENT_CREATOR = new EventCreator.Builder(NO_MORE_DATA_TAG, NoMoreDataEvent.VERSION)
        .withOptionalField(RECORD_COUNT)
        .withOptionalField(ERROR_COUNT)
        .withOptionalField(FILE_COUNT)
        .build();
}



