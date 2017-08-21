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
import com.esotericsoftware.kryo.io.Output;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.io.OutputStream;

public class KryoRecordWriter implements RecordWriter {
  private final Kryo kryo;
  private final Output output;
  private boolean closed;

  public KryoRecordWriter(OutputStream outputStream) throws IOException {
    kryo = new Kryo();
    output = new Output(outputStream);
  }

  @Override
  public String getEncoding() {
    return RecordEncoding.KRYO1.name();
  }

  @Override
  public void write(Record record) throws IOException {
    if (closed) {
      throw new IOException("output has been closed");
    }
    Utils.checkNotNull(record, "record");
    kryo.writeObject(output, record);
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("output has been closed");
    }
    output.flush();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      output.close();
    }
  }
}
