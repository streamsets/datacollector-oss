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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroparquet;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.RecordEL;

public class AvroParquetConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Input Avro File",
    description = "Absolute path to the input avro file on HDFS.",
    defaultValue = "${record:value('/filepath')}",
    displayPosition = 10,
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class}
  )
  public String inputFile;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Output Directory",
    description = "Absolute path to the destination directory on HDFS.",
    defaultValue = "",
    displayPosition = 20,
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class}
  )
  public String outputDirectory;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Keep Avro Input File",
    description = "If checked, the input file will not be removed after the MapReduce converter job ends.",
    defaultValue = "false",
    displayPosition = 10,
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET"
  )
  public boolean keepInputFile;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Compression Codec",
    description = "Compression codec that will be used in Parquet. Valid values are for example 'SNAPPY' or 'LZO'. Empty value will use Parquet default.",
    defaultValue = "",
    displayPosition = 30,
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
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET"
  )
  public int maxPaddingSize = -1;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Overwrite Temporary File",
    description = "If the temporary file exists, overwrite it.",
    defaultValue = "false",
    displayPosition = 80,
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET"
  )
  public boolean overwriteTmpFile = false;
}
