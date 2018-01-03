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
package com.streamsets.pipeline.lib.generator.json;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;

import java.io.IOException;
import java.io.Writer;

public class JsonCharDataGenerator implements DataGenerator {

  private final JsonRecordWriter recordWriter;
  private final Mode mode;

  public JsonCharDataGenerator(ProtoConfigurableEntity.Context context, Writer writer, Mode mode) throws IOException {
    this.mode = mode;
    ContextExtensions ext = ((ContextExtensions) context);
    recordWriter = ext.createJsonRecordWriter(writer, mode);
  }

  @VisibleForTesting
  boolean isArrayObjects() {
    return mode == Mode.ARRAY_OBJECTS;
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    try {
      recordWriter.write(record);
    } catch (IOException e) {
      if (e.getMessage().contains("FileRef")) {
        throw new DataGeneratorException(Errors.JSON_GENERATOR_01);
      } else {
        throw e;
      }
    }
  }

  @Override
  public void flush() throws IOException {
    recordWriter.flush();
  }

  @Override
  public void close() throws IOException {
    recordWriter.close();
  }
}
