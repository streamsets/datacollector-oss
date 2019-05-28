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
    type = FinishedFileEvent.FINISHED_FILE_TAG,
    description = "Generated when the origin finishes processing a file.",
    version = FinishedFileEvent.VERSION
)
public class FinishedFileEvent {
    public static final String FINISHED_FILE_TAG = "finished-file";
    public static final int VERSION = 2;

    @EventFieldDef(
        name = FinishedFileEvent.FILE_PATH,
        description = "Path and name of the file that the origin finished processing."
    )
    public static final String FILE_PATH = "filepath";

    @EventFieldDef(
        name = FinishedFileEvent.RECORD_COUNT,
        description = "Number of records successfully generated from the file.",
        optional = true
    )
    public static final String RECORD_COUNT = "record-count";

    @EventFieldDef(
        name = FinishedFileEvent.ERROR_COUNT,
        description = "Number of error records generated from the file.",
        optional = true
    )
    public static final String ERROR_COUNT = "error-count";

    public static EventCreator EVENT_CREATOR = new EventCreator.Builder(FINISHED_FILE_TAG, FinishedFileEvent.VERSION)
        .withRequiredField(FILE_PATH)
        .withOptionalField(RECORD_COUNT)
        .withOptionalField(ERROR_COUNT)
        .build();
}



