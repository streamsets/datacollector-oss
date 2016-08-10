/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroparquet;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.RecordEL;

public class AvroParquetConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Input file",
    description = "Absolute path to the input file on HDFS.",
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
    label = "Output directory",
    description = "Absolute path to the destination directory on HDFS.",
    defaultValue = "",
    displayPosition = 10,
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
    label = "Keep input file",
    description = "If checked, the input file will not be removed after the mapreduce converter job ends.",
    defaultValue = "false",
    displayPosition = 10,
    group = "AVRO_PARQUET",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_PARQUET"
  )
  public boolean keepInputFile;

}
