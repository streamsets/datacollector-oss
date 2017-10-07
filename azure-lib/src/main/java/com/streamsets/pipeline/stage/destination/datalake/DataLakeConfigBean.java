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
package com.streamsets.pipeline.stage.destination.datalake;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.SdcEL;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.List;


public class DataLakeConfigBean {
  public static final String ADLS_CONFIG_BEAN_PREFIX = "dataLakeConfig.";
  public static final String ADLS_CONFIG_ACCOUNT_FQDN = ADLS_CONFIG_BEAN_PREFIX + "accountFQDN";
  public static final String ADLS_CONFIG_CLIENT_KEY = ADLS_CONFIG_BEAN_PREFIX + "clientKey";
  public static final String ADLS_CONFIG_CLIENT_ID = ADLS_CONFIG_BEAN_PREFIX + "clientId";
  public static final String ADLS_CONFIG_AUTH_ENDPOINT = ADLS_CONFIG_BEAN_PREFIX + "authTokenEndpoint";

  private static final String ADLS_DATA_FORMAT_CONFIG_PREFIX = ADLS_CONFIG_BEAN_PREFIX + "dataFormatConfig";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Application ID",
      description = "Azure Application ID.",
      displayPosition = 10,
      group = "#0"
  )
  public CredentialValue clientId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "https://login.microsoftonline.com/example-example",
      label = "Auth Token Endpoint",
      description = "Azure Auth Token Endpoint.",
      displayPosition = 20,
      group = "#0"
  )
  public CredentialValue authTokenEndpoint;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "example.azuredatalakestore.net",
      label = "Account FQDN",
      description = "full account FQDN, not just the account name.",
      displayPosition = 30,
      group = "#0"
  )
  public CredentialValue accountFQDN;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Application Key",
      description = "Azure Application Key.",
      displayPosition = 40,
      group = "#0"
  )
  public CredentialValue clientKey;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      elDefs = SdcEL.class,
      defaultValue = "sdc-${sdc:id()}",
      label = "Files Prefix",
      displayPosition = 100,
      group = "OUTPUT"
  )
  public String uniquePrefix;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Files Suffix",
      defaultValue = "",
      description = "File name suffix e.g.'txt'",
      displayPosition = 101,
      group = "OUTPUT",
      dependsOn = "dataFormat",
      triggeredByValue = {"TEXT", "JSON", "DELIMITED", "AVRO", "BINARY", "PROTOBUF"}
  )
  public String fileNameSuffix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Directory in Header",
      description = "The directory is defined by the '" + DataLakeTarget.TARGET_DIRECTORY_HEADER + "' record header attribute instead of the Directory Template configuration property.",
      displayPosition = 102,
      group = "OUTPUT"
  )
  public boolean dirPathTemplateInHeader;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "/tmp/out/${YYYY()}-${MM()}-${DD()}-${hh()}",
      label = "Directory Template",
      displayPosition = 110,
      group = "OUTPUT",
      dependsOn = "dirPathTemplateInHeader",
      triggeredByValue = "false"
  )
  public String dirPathTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Data Time Zone",
      description = "Time zone to use to resolve the date time of a time-based partition prefix",
      displayPosition = 120,
      group = "OUTPUT"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${time:now()}",
      label = "Time Basis",
      description = "Time basis to use for a record. Enter an expression that evaluates to a datetime. To use the " +
          "processing time, enter ${time:now()}. To use field values, use '${record:value(\"<field path>\")}'.",
      displayPosition = 130,
      group = "OUTPUT",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String timeDriver;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Max Records in File",
      description = "Number of records that triggers the creation of a new file. Use 0 to opt out.",
      displayPosition = 135,
      group = "OUTPUT",
      min = 0,
      dependsOn = "dataFormat",
      triggeredByValue = {"TEXT", "JSON", "DELIMITED", "AVRO", "BINARY", "PROTOBUF"}
  )
  public long maxRecordsPerFile;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Max File Size (MB)",
      description = "Exceeding this size triggers the creation of a new file. Use 0 to opt out.",
      displayPosition = 136,
      group = "OUTPUT",
      min = 0,
      dependsOn = "dataFormat",
      triggeredByValue = {"TEXT", "JSON", "DELIMITED", "AVRO", "BINARY", "PROTOBUF"}
  )
  public long maxFileSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${1 * HOURS}",
      label = "Idle Timeout",
      description = "Maximum time for a file to remain idle. After no records are written to a file for the" +
          " specified time, the destination closes the file. Enter a number to specify a value in seconds. You" +
          " can also use the MINUTES or HOURS constants in an expression. Use -1 to opt out of a timeout.",
      group = "OUTPUT",
      displayPosition = 137,
      elDefs = {TimeEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "dataFormat",
      triggeredByValue = {"TEXT", "JSON", "DELIMITED", "AVRO", "BINARY", "PROTOBUF"}
  )
  public String idleTimeout;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Roll Attribute",
      description = "Closes the current file and creates a new file when processing a record with the specified roll attribute",
      displayPosition = 138,
      group = "OUTPUT"
  )
  public boolean rollIfHeader;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "roll",
      label = "Roll Attribute Name",
      description = "Name of the roll attribute",
      displayPosition = 138,
      group = "OUTPUT",
      dependsOn = "rollIfHeader",
      triggeredByValue = "true"
  )
  public String rollHeaderName;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Validate Directory Permissions",
      description = "When checked, ADLS destination will create test file in configured target directory to verify access privileges.",
      displayPosition = 140,
      group = "OUTPUT"
  )
  public boolean checkPermission;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data format to use when writing records to Azure Data Lake Store",
      displayPosition = 200,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = {"DATALAKE"})
  public DataGeneratorFormatConfig dataFormatConfig = new DataGeneratorFormatConfig();

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (uniquePrefix == null) {
      uniquePrefix = "";
    }

    if (fileNameSuffix == null) {
      fileNameSuffix = "";
    }
    dataFormatConfig.init(
        context,
        dataFormat,
        Groups.DATALAKE.name(),
        ADLS_DATA_FORMAT_CONFIG_PREFIX,
        issues
    );
  }
}
