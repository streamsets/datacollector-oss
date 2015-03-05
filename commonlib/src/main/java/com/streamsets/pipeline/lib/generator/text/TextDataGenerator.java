/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.text;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;

import java.io.IOException;
import java.io.Writer;

public class TextDataGenerator implements DataGenerator {
  final static String EOL = System.getProperty("line.separator");

  private final String fieldPath;
  private final boolean emptyLineIfNull;
  private final Writer writer;
  private boolean closed;

  public TextDataGenerator(Writer writer, String fieldPath, boolean emptyLineIfNull)
      throws IOException {
    this.writer = writer;
    this.fieldPath = fieldPath;
    this.emptyLineIfNull = emptyLineIfNull;
  }

  //VisibleForTesting
  String getFieldPath() {
    return fieldPath;
  }

  //VisibleForTesting
  boolean isEmptyLineIfNull() {
    return emptyLineIfNull;
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    if (closed) {
      throw new IOException("Generator has been closed");
    }
    Field field = record.get(fieldPath);
    if (field != null) {
      String value;
      try {
        value = field.getValueAsString();
      } catch (Exception ex) {
        throw new DataGeneratorException(Errors.TEXT_GENERATOR_00, record.getHeader().getSourceId(), fieldPath);
      }
      writer.write(value);
      writer.write(EOL);
    } else if (emptyLineIfNull) {
      writer.write(EOL);
    }
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("Generator has been closed");
    }
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    writer.close();
  }
}
