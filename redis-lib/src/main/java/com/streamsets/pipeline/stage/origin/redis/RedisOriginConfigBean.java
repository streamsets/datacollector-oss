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
package com.streamsets.pipeline.stage.origin.redis;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.DataFormatChooserValues;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.pipeline.stage.origin.redis.configuration.AdvancedConfig;
import com.streamsets.pipeline.stage.origin.redis.configuration.ReadStrategy;
import com.streamsets.pipeline.stage.origin.redis.configuration.ReadStrategyChooserValues;

import java.util.List;

public class RedisOriginConfigBean {

    public static final String DATA_FROMAT_CONFIG_BEAN_PREFIX = "dataFormatConfig.";

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "URI",
            description = "Use format redis://[username:password@]host[:port][/[database]]",
            displayPosition = 10,
            group = "REDIS"
    )
    public String uri = "redis://:password@localhost:6379/0";

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "BATCH",
            label = "Read strategy",
            description = "Redis read strategy",
            displayPosition = 20,
            group = "REDIS"
    )
    @ValueChooserModel(ReadStrategyChooserValues.class)
    public ReadStrategy readStrategy;

    @ConfigDefBean(groups = "REDIS")
    public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            label = "Data Format",
            description = "Format of data",
            displayPosition = 30,
            group = "REDIS"
    )
    @ValueChooserModel(DataFormatChooserValues.class)
    public DataFormat dataFormat;

    @ConfigDef(
            type = ConfigDef.Type.STRING,
            label = "Key name",
            description = "Key name to read from",
            defaultValue = "",
            required = true,
            group = "BATCH",
            displayPosition = 10,
            dependsOn = "readStrategy",
            triggeredByValue = "BATCH"
    )
    public String queueName;

    @ConfigDef(
            type = ConfigDef.Type.LIST,
            label = "Channel(s)",
            description = "Channel(s) to subscribe to",
            required = false,
            group = "SUBSCRIPTION",
            displayPosition = 10,
            dependsOn = "readStrategy",
            triggeredByValue = "SUBSCRIPTION"

    )
    public List<String> subscriptionChannels;

    @ConfigDef(
            type = ConfigDef.Type.LIST,
            label = "Pattern",
            description = "Subscribes to messages matching the given patterns",
            required = false,
            group = "SUBSCRIPTION",
            displayPosition = 20,
            dependsOn = "readStrategy",
            triggeredByValue = "SUBSCRIPTION"
    )
    public List<String> subscriptionPatterns;

    @ConfigDef(
            type = ConfigDef.Type.NUMBER,
            label = "Wait time",
            defaultValue = "",
            description = "Max wait time for batch",
            required = false,
            group = "SUBSCRIPTION",
            displayPosition = 30,
            dependsOn = "readStrategy",
            triggeredByValue = "SUBSCRIPTION"
    )
    public int maxWaitTime;

    @ConfigDefBean(groups = {"ADVANCED"})
    public AdvancedConfig advancedConfig;
}