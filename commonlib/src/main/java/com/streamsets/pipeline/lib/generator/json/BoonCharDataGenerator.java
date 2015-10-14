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
package com.streamsets.pipeline.lib.generator.json;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import org.boon.json.JsonFactory;
import org.boon.json.JsonSerializer;
import org.boon.json.ObjectMapper;
import org.boon.primitive.CharBuf;

import java.io.IOException;
import java.io.Writer;

public class BoonCharDataGenerator implements DataGenerator {
  final static String EOL = System.getProperty("line.separator");

  private Writer writer;
  private ObjectMapper mapper =  JsonFactory.create();
  private boolean closed;

  public BoonCharDataGenerator(Writer writer, JsonMode jsonMode)
      throws IOException {
    if (jsonMode != JsonMode.MULTIPLE_OBJECTS) {
      throw new IllegalArgumentException("This generator only supports MULTIPLE_OBJECTS");
    }
    this.writer = writer;
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    JsonSerializer serializer = mapper.serializer();
    CharBuf serialize = serializer.serialize(JsonUtil.fieldToJsonObject(record, record.get()));
    int length = serialize.length();
    writer.write(serialize.readForRecycle(), 0 , length);
    writer.write(EOL);
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("generator has been closed");
    }
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    writer.close();
  }
}
