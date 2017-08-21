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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.RecordReader;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

public class KryoRecordReader implements RecordReader {
  private final Kryo kryo;
  private final Input input;
  private boolean closed;

  public KryoRecordReader(InputStream inputStream, long initialPosition) throws IOException {
    kryo = new Kryo();
    IOUtils.skipFully(inputStream, initialPosition);
    input = new Input(inputStream);
    input.setTotal(initialPosition);
  }

  @Override
  public String getEncoding() {
    return RecordEncoding.KRYO1.name();
  }

  @Override
  public long getPosition() {
    return input.total();
  }

  @Override
  public Record readRecord() throws IOException {
    if (closed) {
      throw new IOException("input has been closed");
    }
    return input.eof() ? null : kryo.readObject(input, RecordImpl.class);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      input.close();
    }
  }

}
