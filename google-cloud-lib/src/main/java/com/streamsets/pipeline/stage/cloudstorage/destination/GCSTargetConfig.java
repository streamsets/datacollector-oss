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

package com.streamsets.pipeline.stage.cloudstorage.destination;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.googlecloud.CloudStorageCredentialsConfig;
import com.streamsets.pipeline.stage.cloudstorage.lib.Errors;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.List;

public class GCSTargetConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Bucket",
      description = "Expression that will identify bucket for each record.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      //TODO SDC-7719
      group = "GCS"
  )
  public String bucketTemplate;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Common Prefix",
      description = "",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "GCS"
  )
  public String commonPrefix;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      label = "Partition Prefix",
      description = "Partition to write to. If the partition doesn't exist on GCS, it will be created.",
      displayPosition = 180,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "GCS"
  )
  public String partitionTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Data Time Zone",
      description = "Time zone to use to resolve the date time of a time-based partition prefix",
      displayPosition = 190,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GCS"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "${time:now()}",
      label = "Time Basis",
      description = "Time basis to use for a record. Enter an expression that evaluates to a datetime. To use the " +
          "processing time, enter ${time:now()}. To use field values, use '${record:value(\"<filepath>\")}'.",
      displayPosition = 200,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GCS"
  )
  public String timeDriverTemplate;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "sdc",
      description = "Prefix for object names that will be uploaded on GCS",
      label = "Object Name Prefix",
      displayPosition = 210,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GCS"
  )
  public String fileNamePrefix;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      description = "Suffix for object names that will be uploaded on GCS. e.g.'txt'",
      label = "Object Name Suffix",
      displayPosition = 220,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GCS",
      dependsOn = "dataFormat",
      triggeredByValue = {"TEXT", "JSON", "DELIMITED", "AVRO", "BINARY", "PROTOBUF", "SDC_JSON", "XML"}
  )
  public String fileNameSuffix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  @ConfigDefBean(groups = "CREDENTIALS")
  public CloudStorageCredentialsConfig credentials = new CloudStorageCredentialsConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Compress with gzip",
      displayPosition = 230,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GCS",
      dependsOn = "dataFormat",
      triggeredByValue = {"TEXT", "JSON", "DELIMITED", "AVRO", "XML", "PROTOBUF", "SDC_JSON"}
  )
  public boolean compress;

  public List<Stage.ConfigIssue> init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    dataGeneratorFormatConfig.init(
        context,
        dataFormat,
        "GCS",
        "gcsTargetConfig.dataFormat",
        issues
    );
    //File Suffix should not contain '/' or start with '.'
    if (fileNameSuffix != null && (fileNameSuffix.startsWith(".") || fileNameSuffix.contains("/"))) {
      issues.add(
          context.createConfigIssue(
              Groups.GCS.getLabel(),
              "gcsTargetConfig.fileNameSuffix",
              Errors.GCS_05
          )
      );
    }
    return issues;
  }
}
