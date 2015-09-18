/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.lib.generator.avro;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.io.OutputStream;

public class AvroDataOutputStreamGenerator implements DataGenerator {

  private final Schema schema;
  private boolean closed;
  private final DataFileWriter<GenericRecord> dataFileWriter;

  public AvroDataOutputStreamGenerator(OutputStream outputStream, String avroSchema)
      throws IOException {
    schema = new Schema.Parser().setValidate(true).parse(avroSchema);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, outputStream);
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    try {
      dataFileWriter.append((GenericRecord)AvroTypeUtil.sdcRecordToAvro(record, schema));
    } catch (StageException e) {
      throw new DataGeneratorException(e.getErrorCode(), e.getParams()); // params includes cause
    }
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    dataFileWriter.flush();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    dataFileWriter.close();
  }
}
