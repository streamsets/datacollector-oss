/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.record.io;

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
