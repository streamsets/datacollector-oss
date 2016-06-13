/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.redis.configuration;

import com.streamsets.pipeline.api.ConfigDef;

public class AdvancedConfig {
    @ConfigDef(
            type = ConfigDef.Type.STRING,
            label = "Keys glob-style pattern",
            defaultValue = "*",
            required = true,
            group = "ADVANCED",
            displayPosition = 10
    )
    public String keysPattern;

    @ConfigDef(
            type = ConfigDef.Type.STRING,
            label = "Namespace separator",
            defaultValue = ":",
            required = true,
            group = "ADVANCED",
            displayPosition = 20
    )
    public String namespaceSeparator;

    @ConfigDef(
            type = ConfigDef.Type.NUMBER,
            label = "Connection timeout",
            description = "Connection timeout (sec)",
            defaultValue = "60",
            required = true,
            min = 1,
            group = "ADVANCED",
            displayPosition = 30
    )
    public int connectionTimeout;

    @ConfigDef(
            type = ConfigDef.Type.NUMBER,
            label = "Execution timeout",
            description = "Execution timeout (sec)",
            defaultValue = "60",
            required = true,
            min = 1,
            group = "ADVANCED",
            displayPosition = 40
    )
    public int executionTimeout;
}
