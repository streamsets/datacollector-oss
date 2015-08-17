/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.binary;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;

public class BinaryDataGenerator implements DataGenerator {

  private final String fieldPath;
  private boolean closed;
  private final OutputStream outputStream;

  public BinaryDataGenerator(OutputStream outputStream, String fieldPath)
      throws IOException {
    this.outputStream = outputStream;
    this.fieldPath = fieldPath;
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    if (closed) {
      throw new IOException("generator has been closed");
    }

    Field field = record.get(fieldPath);
    if (field != null && field.getValue() != null) {
      byte[] value;
      try {
        value = field.getValueAsByteArray();
      } catch (IllegalArgumentException  ex) {
        throw new DataGeneratorException(Errors.BINARY_GENERATOR_00, record.getHeader().getSourceId(), fieldPath);
      }

      try {
        IOUtils.write(value, outputStream);
      } catch (IOException ex) {
        throw new DataGeneratorException(Errors.BINARY_GENERATOR_01, record.getHeader().getSourceId(), ex.toString());
      }
    }
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    outputStream.close();
  }

  public String getFieldPath() {
    return fieldPath;
  }
}
