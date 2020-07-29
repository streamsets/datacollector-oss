/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.RecordEL;

public class AvroConversionCommonConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Input Avro File",
    description = "Absolute path to the input avro file on HDFS.",
    defaultValue = "${record:value('/filepath')}",
    displayPosition = 10,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "AVRO_CONVERSION",
    dependsOn = "jobType^",
    triggeredByValue = {"AVRO_PARQUET", "AVRO_ORC"},
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
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "AVRO_CONVERSION",
    dependsOn = "jobType^",
    triggeredByValue = {"AVRO_PARQUET", "AVRO_ORC"},
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
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "AVRO_CONVERSION",
    dependsOn = "jobType^",
    triggeredByValue = {"AVRO_PARQUET", "AVRO_ORC"}
  )
  public boolean keepInputFile;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Overwrite Temporary File",
    description = "If the temporary file exists, overwrite it.",
    defaultValue = "false",
    displayPosition = 80,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "AVRO_CONVERSION",
    dependsOn = "jobType^",
    triggeredByValue = {"AVRO_PARQUET", "AVRO_ORC"}
  )
  public boolean overwriteTmpFile = false;
}
