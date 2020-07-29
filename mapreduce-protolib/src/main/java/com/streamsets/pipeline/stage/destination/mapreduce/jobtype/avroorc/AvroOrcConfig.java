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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroorc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.util.avroorc.AvroToOrcRecordConverter;

public class AvroOrcConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    label = "ORC Batch Size",
    description = "Number of records that will be written per ORC writer batch.",
    defaultValue = "" + AvroToOrcRecordConverter.DEFAULT_ORC_BATCH_SIZE,
    displayPosition = 100,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = "AVRO_ORC",
    dependsOn = "jobType^",
    triggeredByValue = "AVRO_ORC"
  )
  public int orcBatchSize = AvroToOrcRecordConverter.DEFAULT_ORC_BATCH_SIZE;
}
