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
package com.streamsets.pipeline.stage.processor.transformer;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.el.SdcEL;
import com.streamsets.pipeline.lib.converter.AvroParquetConfig;
import com.streamsets.pipeline.lib.el.DataUnitsEL;
import com.streamsets.pipeline.lib.el.RecordEL;

public class JobConfig {
  public static final String TEMPDIR = "jobConfig.tempDir";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Job Type",
      description = "Type of job that will be executed.",
      defaultValue = "AVRO_PARQUET",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "JOB"
  )
  @ValueChooserModel(JobTypeChooserValues.class)
  public JobType jobType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/tmp/out/.parquet",
      label = "Temporary File Directory",
      description = "Directory for the temporary Parquet files",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "JOB",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String tempDir = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = ".avro_to_parquet_tmp_conversion_",
      label = "Files Prefix",
      description = "File name prefix",
      displayPosition = 21,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "JOB",
      elDefs = SdcEL.class
  )
  public String uniquePrefix;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = ".parquet",
      label = "Files Suffix",
      description = "File name suffix e.g.'.parquet'",
      displayPosition = 22,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "JOB"
  )
  public String fileNameSuffix;

  //Whole File
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "8192",
      label = "Buffer Size (bytes)",
      description = "Size of the Buffer used to copy the file.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "JOB",
      min = 1,
      max = Integer.MAX_VALUE
  )
  //Optimal 8KB
  public int wholeFileMaxObjectLen = 8 * 1024;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "-1",
      label = "Rate per second",
      description = "Rate / sec to manipulate bandwidth requirements for File Transfer." +
          " Use <= 0 to opt out. Default unit is B/sec",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "JOB",
      elDefs = {DataUnitsEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String rateLimit = "-1";

  @ConfigDefBean
  public AvroParquetConfig avroParquetConfig;
}
