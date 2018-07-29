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
package com.streamsets.pipeline.lib.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverterLogicalTypesPre19;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

/**
 * Custom Builder to inject our own writer support that can work with logical types in older parquet version(s). The
 * logic is functionally equivalent to AvroParquetWriter.Builder
 */
public class AvroParquetWriterBuilder<T> extends ParquetWriter.Builder<T, AvroParquetWriterBuilder<T>> {
  private Schema schema;
  private GenericData model;

  public AvroParquetWriterBuilder(Path file) {
    super(file);
    this.schema = null;
    this.model = SpecificData.get();
  }

  public AvroParquetWriterBuilder<T> withSchema(Schema schema) {
    this.schema = schema;
    return this;
  }

  public AvroParquetWriterBuilder<T> withDataModel(GenericData model) {
    this.model = model;
    return this;
  }

  protected AvroParquetWriterBuilder<T> self() {
    return this;
  }

  protected WriteSupport<T> getWriteSupport(Configuration conf) {
    return new AvroWriteSupport((new AvroSchemaConverterLogicalTypesPre19(conf)).convert(this.schema), this.schema, this.model);
  }
}
