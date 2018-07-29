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
package com.streamsets.pipeline.stage.destination.mapreduce.config;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.SimpleJobCreator;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroorc.AvroOrcConvertCreator;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroparquet.AvroParquetConvertCreator;

public enum JobType implements Label {
  CUSTOM("Custom", ""),
  SIMPLE("From Configuration object", SimpleJobCreator.class.getCanonicalName()),
  AVRO_PARQUET("Convert Avro to Parquet", AvroParquetConvertCreator.class.getCanonicalName()),
  AVRO_ORC("Convert Avro to ORC", AvroOrcConvertCreator.class.getCanonicalName()),
  ;

  private final String klass;
  private final String label;
  JobType(String label, String klass) {
    this.label = label;
    this.klass = klass;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public String getKlass() {
    return  klass;
  }
}
