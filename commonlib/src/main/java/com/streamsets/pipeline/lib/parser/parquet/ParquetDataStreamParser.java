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
package com.streamsets.pipeline.lib.parser.parquet;

import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;

import java.io.IOException;
import java.io.InputStream;

public class ParquetDataStreamParser extends AbstractDataParser {
  private ProtoConfigurableEntity.Context context;

  @Override
  public Record parse() throws IOException, DataParserException {
    InputStream in = null;

    AvroParquetReader.<GenericRecord>builder(null).build();


    Record record = context.createRecord("test");
    return record;
  }

  @Override
  public String getOffset() throws DataParserException {
    return "";
  }

  @Override
  public void close() throws IOException {

  }
}
