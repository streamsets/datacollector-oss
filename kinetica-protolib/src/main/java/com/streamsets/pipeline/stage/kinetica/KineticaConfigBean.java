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
package com.streamsets.pipeline.stage.kinetica;

import com.streamsets.pipeline.api.ConfigDef;

public class KineticaConfigBean {

  // Connection Tab

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "http://127.0.0.1:9191",
    label = "Kinetica URL",
    description = "URL of the Kinetica Head Node",
    displayPosition = 10, group = "CONNECTION"
  )
  public String url;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "10000",
    label = "Batch Size",
    description = "Bulk Inserter batch size",
    displayPosition = 20, group = "CONNECTION"
  )
  public int batchSize;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.BOOLEAN,
    label = "Transport Compression",
    description = "Compress data sent to Kinetica",
    displayPosition = 30,
    group = "CONNECTION"
  )
  public boolean transportCompressionEnabled;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.BOOLEAN,
    label = "Disable Multihead Ingest",
    description = "If set, all inserts wil be routed to the Kinetica Head Node. If not set, multihead ingest will be used when applicable for sharded tables",
    displayPosition = 40,
    group = "CONNECTION"
  )
  public boolean disableMultihead;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "IP Regex",
    description = "Regex expression to filter Kinetica Host names",
    displayPosition = 50,
    group = "CONNECTION"
  )
  public String ipRegex;

  // Table Tab

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "",
    label = "Table Name",
    displayPosition = 10,
    group = "TABLE"
  )
  public String tableName;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Update On Existing PK",
    description = "If set, inserted rows with existing primary key will perform updates; if false, inserted rows with existing primary key will be ignored",
    displayPosition = 20,
    group = "TABLE"
  )
  public boolean updateOnExistingPk;

  // Credentials Tab

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "",
    label = "Username",
    displayPosition = 10,
    group = "CREDENTIALS"
  )
  public String username;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "",
    label = "Password",
    displayPosition = 20,
    group = "CREDENTIALS"
  )
  public String password;

}
