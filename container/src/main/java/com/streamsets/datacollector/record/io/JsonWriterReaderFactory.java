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
package com.streamsets.datacollector.record.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.json.JsonObjectReaderImpl;
import com.streamsets.datacollector.json.JsonRecordWriterImpl;
import com.streamsets.datacollector.json.OverrunJsonObjectReaderImpl;
import com.streamsets.pipeline.api.ext.JsonObjectReader;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.ext.json.Mode;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

public class JsonWriterReaderFactory {

  private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();

  private JsonWriterReaderFactory() {}

  public static JsonObjectReader createObjectReader(
      Reader reader,
      long initialPosition,
      Mode mode,
      Class<?> objectClass,
      int maxObjectLen
  ) throws IOException {
    // overrun
    return new OverrunJsonObjectReaderImpl(
        new OverrunReader(
            reader,
            OverrunReader.getDefaultReadLimit(),
            false,
            false
        ),
        initialPosition,
        maxObjectLen,
        mode,
        objectClass
    );
  }

  public static JsonObjectReader createObjectReader(
      Reader reader,
      long initialPosition,
      Mode mode,
      Class<?> objectClass
  ) throws IOException {
    // non overrun
    return new JsonObjectReaderImpl(
        reader,
        initialPosition,
        mode,
        objectClass
    );
  }

  public static JsonRecordWriter createRecordWriter(
      Writer writer,
      Mode mode
  ) throws IOException {
    return new JsonRecordWriterImpl(
        writer,
        mode
    );
  }

  public static ObjectMapper getDefaultObjectMapper() {
    return DEFAULT_OBJECT_MAPPER;
  }
}
