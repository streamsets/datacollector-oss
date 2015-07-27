/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
