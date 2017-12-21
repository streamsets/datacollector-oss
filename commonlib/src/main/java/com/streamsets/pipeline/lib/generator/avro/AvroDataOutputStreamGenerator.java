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
package com.streamsets.pipeline.lib.generator.avro;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.util.AvroJavaSnappyCodec;
import com.streamsets.pipeline.lib.util.AvroSchemaHelper;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class AvroDataOutputStreamGenerator extends BaseAvroDataGenerator {

  static {
    // replace Avro Snappy codec with SDC's which is 100% Java
    AvroJavaSnappyCodec.initialize();
  }

  private OutputStream outputStream;
  private String compressionCodec;
  private DataFileWriter<GenericRecord> dataFileWriter;

  public AvroDataOutputStreamGenerator(
      boolean schemaInHeader,
      OutputStream outputStream,
      String compressionCodec,
      Schema schema,
      Map<String, Object> defaultValueMap,
      String schemaSubject,
      AvroSchemaHelper schemaHelper,
      int schemaId
  ) throws IOException {
    super(schemaInHeader, schema, defaultValueMap, schemaHelper, schemaSubject, schemaId);
    this.compressionCodec = compressionCodec;
    this.outputStream = outputStream;
    if(!schemaInHeader) {
      initialize();
    }
  }

  @Override
  protected void initializeWriter() throws IOException {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.setCodec(CodecFactory.fromString(compressionCodec));
    dataFileWriter.create(schema, outputStream);
  }

  @Override
  protected void writeRecord(Record record) throws IOException, DataGeneratorException {
    try {
      dataFileWriter.append((GenericRecord)AvroTypeUtil.sdcRecordToAvro(record, schema, defaultValueMap));
    } catch (StageException e) {
      throw new DataGeneratorException(e.getErrorCode(), e.getParams()); // params includes cause
    }
  }

  @Override
  protected Flushable getFlushable() {
    return dataFileWriter;
  }

  @Override
  protected Closeable getCloseable() {
    return dataFileWriter;
  }
}
