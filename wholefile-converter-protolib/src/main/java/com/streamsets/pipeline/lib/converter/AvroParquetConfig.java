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
package com.streamsets.pipeline.lib.converter;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.TimeZoneChooserValues;

public class AvroParquetConfig {

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Compression Codec",
    description = "Compression codec that will be used in Parquet. Valid values are for example 'SNAPPY' or 'LZO'. Empty value will use Parquet default.",
    defaultValue = "",
    displayPosition = 30,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET"
  )
  public String compressionCodec = "";

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    label = "Row Group Size",
    description = "Row group size that will be used in Parquet. Use -1 to use the Parquet default.",
    defaultValue = "-1",
    displayPosition = 40,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET"
  )
  public int rowGroupSize = -1;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    label = "Page Size",
    description = "Page size that will be used in Parquet. Use -1 to use the Parquet default.",
    defaultValue = "-1",
    displayPosition = 50,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET"
  )
  public int pageSize = -1;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    label = "Dictionary Page Size",
    description = "Dictionary page size that will be used in Parquet. Use -1 to use the Parquet default.",
    defaultValue = "-1",
    displayPosition = 60,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET"
  )
  public int dictionaryPageSize = -1;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    label = "Max Padding Size",
    description = "Max padding size that will be used in Parquet. Use -1 to use the Parquet default.",
    defaultValue = "-1",
    displayPosition = 70,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET"
  )
  public int maxPaddingSize = -1;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Data Time Zone",
      description = "Time zone to use for a record.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "AVRO_PARQUET",
      dependsOn = "jobType^",
      triggeredByValue = "AVRO_PARQUET"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID = "UTC";

}
