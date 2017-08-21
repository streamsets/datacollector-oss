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
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;

public class TargetFieldHasherConfig extends FieldHasherConfig {
    @ConfigDef(
        required = false,
        type = ConfigDef.Type.MODEL,
        defaultValue = "",
        label = "Target Field",
        description = "String field to store the hashed value. Creates the field if it does not exist.",
        group = "RECORD_HASHING",
        displayPosition = 30
    )
    @FieldSelectorModel(singleValued = true)
    public String targetField;

    @ConfigDef(
        required = false,
        type = ConfigDef.Type.STRING,
        defaultValue = "",
        label = "Header Attribute",
        description = "Header attribute to store the hashed value. Creates the attribute if it does not exist.",
        group = "RECORD_HASHING",
        displayPosition = 40
    )
    public String headerAttribute;
}
