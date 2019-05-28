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
    type = NewFileEvent.NEW_FILE_TAG,
    description = "Generated when the origin starts processing a new file.",
    version = NewFileEvent.VERSION
)
public class NewFileEvent {
    public static final String NEW_FILE_TAG = "new-file";
    public static final int VERSION = 1;

    @EventFieldDef(
        name = NewFileEvent.FILE_PATH,
        description = "Path and name of the file that the origin started processing."
    )
    public static final String FILE_PATH = "filepath";

    public static EventCreator EVENT_CREATOR = new EventCreator.Builder(NEW_FILE_TAG, NewFileEvent.VERSION)
        .withRequiredField(FILE_PATH)
        .build();
}



