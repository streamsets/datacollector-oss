/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.pipeline.lib.connection.snowpipe;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

@ConnectionDef(
    label = "Snowpipe",
    type = SnowpipeConnection.TYPE,
    description = "Defines a Snowpipe Connection",
    version = 1,
    upgraderDef = "upgrader/SnowpipeConnectionUpgrader.yaml",
    supportedEngines = { ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER }
)
@ConfigGroups(SnowpipeConnectionGroups.class)
public class SnowpipeConnection {

    public static final String TYPE = "STREAMSETS_SNOWPIPE";

    @ConfigDef(
        required = false,
        type = ConfigDef.Type.CREDENTIAL,
        label = "Private Key PEM",
        description = "Paste PEM here or use a credential store EL to retrieve it. " +
                "Required only if using Snowpipe to ingest data",
        displayPosition = 20,
        group = "SNOWPIPE"
    )
    public CredentialValue privateKeyPem;

    @ConfigDef(
        required = false,
        type = ConfigDef.Type.CREDENTIAL,
        label = "Private Key Password",
        description = "Required only if using Snowpipe to ingest data",
        displayPosition = 30,
        group = "SNOWPIPE"
    )
    public CredentialValue privateKeyPassword;

    @ConfigDef(
        required = false,
        type = ConfigDef.Type.CREDENTIAL,
        label = "Public Key PEM",
        description = "Paste PEM here or use a credential store EL to retrieve it. " +
                "Required only if using Snowpipe to ingest data",
        displayPosition = 40,
        group = "SNOWPIPE"
    )
    public CredentialValue publicKeyPem;

    @ConfigDef(
        required = true,
        defaultValue = "false",
        type = ConfigDef.Type.BOOLEAN,
        label = "Use Custom Snowpipe Endpoint",
        displayPosition = 50,
        group = "SNOWPIPE"
    )
    public boolean useCustomSnowflakeEndpoint;

    @ConfigDef(
        required = true,
        defaultValue = "true",
        type = ConfigDef.Type.BOOLEAN,
        label = "Use TLS",
        displayPosition = 60,
        group = "SNOWPIPE",
        dependencies = {
            @Dependency(configName = "useCustomSnowflakeEndpoint", triggeredByValues = "true")
        }
    )
    public boolean useTls;

    @ConfigDef(
        required = true,
        defaultValue = "[SNOWFLAKE ACCOUNT].snowflakecomputing.com",
        type = ConfigDef.Type.STRING,
        label = "Custom Snowpipe Host",
        displayPosition = 70,
        group = "SNOWPIPE",
        dependencies = {
            @Dependency(configName = "useCustomSnowflakeEndpoint", triggeredByValues = "true")
        }
    )
    public String customSnowflakeHost;

    @ConfigDef(
        required = true,
        defaultValue = "443",
        type = ConfigDef.Type.NUMBER,
        label = "Custom Snowpipe Port",
        displayPosition = 80,
        group = "SNOWPIPE",
        dependencies = {
            @Dependency(configName = "useCustomSnowflakeEndpoint", triggeredByValues = "true")
        }
    )
    public int customSnowflakePort;
}
