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
package com.streamsets.pipeline.stage.destination.influxdb;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.influxdb.InfluxDB;

public class InfluxConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "URL",
      description = "InfluxDB HTTP API URL",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String url = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public CredentialValue username = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public CredentialValue password = () -> ""; // NOSONAR

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Database Name",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String dbName = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Auto-create Database",
      description = "Automatically create a new database with the specified name if it does not already exist.",
      defaultValue = "false",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public boolean autoCreate = false;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Retention Policy",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String retentionPolicy = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Consistency Level",
      defaultValue = "ALL",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  @ValueChooserModel(ConsistencyLevelChooserValues.class)
  public InfluxDB.ConsistencyLevel consistencyLevel;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Record Mapping",
      description = "Converters take incoming records and transform them to an InfluxDB measurement.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  @ValueChooserModel(RecordConverterChooserValues.class)
  public RecordConverterType recordConverterType;

  @ConfigDefBean
  public GenericRecordConverterConfigBean fieldMapping = new GenericRecordConverterConfigBean();
}
