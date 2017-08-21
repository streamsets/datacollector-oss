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
