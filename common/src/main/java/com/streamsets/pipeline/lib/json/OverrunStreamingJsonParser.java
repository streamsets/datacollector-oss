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
package com.streamsets.pipeline.lib.json;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonObjectReader;
import com.streamsets.pipeline.api.ext.json.Mode;

import java.io.IOException;
import java.io.Reader;

/**
 * Caps amount the size of of JSON objects being parsed, discarding the ones that exceed the limit and fast forwarding
 * to the next.
 * <p/>
 * If the max object length is exceed, the readMap(), readList() method will throw a JsonObjectLengthException.
 * After a JsonObjectLengthException exception the parser is still usable, the subsequent read will be positioned for
 * the next object.
 * <p/>
 * The underlying InputStream is wrapped with an OverrunInputStream to prevent an overrun due to an extremely large
 * field name or string value. The default limit is 100K and it is configurable via a JVM property,
 * {@link com.streamsets.pipeline.api.ext.io.OverrunReader.READ_LIMIT_SYS_PROP}, as it is not expected user will need
 * to change this. If an OverrunException is thrown the parser is not usable anymore.
 */
public class OverrunStreamingJsonParser implements StreamingJsonParser {

  private final JsonObjectReader jsonReader;

  public OverrunStreamingJsonParser(Stage.Context context, Reader reader, Mode mode, int maxObjectLength) throws IOException {
    this(context, reader, 0, mode, maxObjectLength);
  }

  public OverrunStreamingJsonParser(Stage.Context context, Reader reader, long initialPosition, Mode mode, int maxObjectLength) throws IOException {
    jsonReader = ((ContextExtensions) context).createJsonObjectReader(
        reader,
        initialPosition,
        maxObjectLength,
        mode,
        Object.class
    );
  }

  @Override
  public long getReaderPosition() {
    return jsonReader.getReaderPosition();
  }

  @Override
  public Object read() throws IOException {
    return jsonReader.read();
  }

  @Override
  public void close() throws IOException {
    jsonReader.close();
  }

}
