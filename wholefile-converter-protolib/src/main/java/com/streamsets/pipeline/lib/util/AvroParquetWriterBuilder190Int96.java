/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter190Int96Avro17;
import org.apache.parquet.avro.AvroSchemaConverter190Int96Avro18;
import org.apache.parquet.avro.AvroWriteSupportInt96Avro17;
import org.apache.parquet.avro.AvroWriteSupportInt96Avro18;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroParquetWriterBuilder190Int96 <T> extends ParquetWriter.Builder<T, AvroParquetWriterBuilder190Int96<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroParquetWriterBuilder190Int96.class);

  private Schema schema;
  private GenericData model;
  private String timeZoneId;

  public AvroParquetWriterBuilder190Int96(Path file) {
    this(file, null);
  }

  public AvroParquetWriterBuilder190Int96(Path file, String timeZoneId) {
    super(file);
    this.schema = null;
    this.model = SpecificData.get();
    this.timeZoneId = timeZoneId;
  }

  public AvroParquetWriterBuilder190Int96<T> withSchema(Schema schema) {
    this.schema = schema;
    return this;
  }

  public AvroParquetWriterBuilder190Int96<T> withDataModel(GenericData model) {
    this.model = model;
    return this;
  }

  protected AvroParquetWriterBuilder190Int96<T> self() {
    return this;
  }

  protected WriteSupport<T> getWriteSupport(Configuration conf) {
    AvroLogicalTypeSupport avroLogicalTypeSupport = AvroLogicalTypeSupport.getAvroLogicalTypeSupport();
    if (avroLogicalTypeSupport.isLogicalTypeSupported()) {
      LOG.debug("Returning write support with converter = AvroSchemaConverter190Int96Avro18");
      return new AvroWriteSupportInt96Avro18<>(
          (new AvroSchemaConverter190Int96Avro18(conf)).convert(this.schema),
          this.schema,
          this.model,
          this.timeZoneId
      );
    } else {
      LOG.debug("Returning write support with converter = AvroSchemaConverter190Int96Avro17");
      return new AvroWriteSupportInt96Avro17<>(
          (new AvroSchemaConverter190Int96Avro17(conf)).convert(this.schema),
          this.schema,
          this.model,
          this.timeZoneId
      );
    }
  }

}
