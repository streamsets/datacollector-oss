/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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

  private Schema schema;
  private boolean closed;
  private DataFileWriter<GenericRecord> dataFileWriter;

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
      throw new DataGeneratorException(e.getErrorCode(), e.getMessage(), e);
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
