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
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.IOException;
import java.io.OutputStream;

public class AvroMessageGenerator implements DataGenerator {

  private Schema schema;
  private boolean closed;
  private DatumWriter<GenericRecord> datumWriter;
  private BinaryEncoder binaryEncoder;
  private final OutputStream outputStream;

  public AvroMessageGenerator(OutputStream outputStream, String avroSchema)
      throws IOException {
    this.outputStream = outputStream;
    schema = new Schema.Parser().setValidate(true).parse(avroSchema);
    datumWriter = new GenericDatumWriter<>(schema);
    binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    try {
      datumWriter.write((GenericRecord) AvroTypeUtil.sdcRecordToAvro(record, schema), binaryEncoder);
    } catch (StageException e) {
      throw new DataGeneratorException(e.getErrorCode(), e.toString(), e);
    }
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    binaryEncoder.flush();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    try {
      binaryEncoder.flush();
    } finally {
      outputStream.close();
    }
  }
}
