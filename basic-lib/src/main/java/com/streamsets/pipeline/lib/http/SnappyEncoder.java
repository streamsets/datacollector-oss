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
package com.streamsets.pipeline.lib.http;

import org.glassfish.jersey.spi.ContentEncoder;
import org.iq80.snappy.SnappyFramedInputStream;
import org.iq80.snappy.SnappyFramedOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SnappyEncoder extends ContentEncoder {
  public SnappyEncoder() {
    super(HttpConstants.SNAPPY_COMPRESSION);
  }

  public InputStream decode(String contentEncoding, InputStream encodedStream) throws IOException {
    return new SnappyFramedInputStream(encodedStream, true);
  }

  public OutputStream encode(String contentEncoding, OutputStream entityStream) throws IOException {
    return new SnappyFramedOutputStream(entityStream);
  }
}
