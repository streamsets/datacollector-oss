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
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.EventDef;
import com.streamsets.pipeline.api.EventFieldDef;

@EventDef(
    type = ErrorEvent.ERROR_TAG,
    description = "Generated when the origin fails to process the file.",
    version = ErrorEvent.VERSION
)
public class ErrorEvent {
    public static final String ERROR_TAG = "ERROR";
    public static final int VERSION = 1;

    @EventFieldDef(
        name = ErrorEvent.FILE_NAME,
        description = "Name of the file that the origin failed to process."
    )
    public static final String FILE_NAME = "fileName";
}



