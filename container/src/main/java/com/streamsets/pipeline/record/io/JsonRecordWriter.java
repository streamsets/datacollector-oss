/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.record.io;

import com.fasterxml.jackson.core.JsonGenerator;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.restapi.bean.BeanHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class JsonRecordWriter implements RecordWriter {
  private final Writer writer;
  private final JsonGenerator generator;
  private boolean closed;

  public JsonRecordWriter(OutputStream outputStream) throws IOException {
    writer = new OutputStreamWriter(outputStream, "UTF-8");
    generator = ObjectMapperFactory.getOneLine().getFactory().createGenerator(writer);
  }

  @Override
  public void write(Record record) throws IOException {
    if (closed) {
      throw new IOException("writer has been closed");
    }
    generator.writeObject(BeanHelper.wrapRecord(record));
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("writer has been closed");
    }
    writer.flush();
  }

  @Override
  public void close() {
    closed = true;
    try {
      writer.close();
    } catch (IOException ex) {
      //NOP
    }
  }
}
