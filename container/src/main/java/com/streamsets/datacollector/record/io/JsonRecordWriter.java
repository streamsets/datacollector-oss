/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.record.io;

import com.fasterxml.jackson.core.JsonGenerator;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.RecordWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class JsonRecordWriter implements RecordWriter {
  private final static Logger LOG = LoggerFactory.getLogger(JsonRecordWriter.class);
  private final Writer writer;
  private final JsonGenerator generator;
  private boolean closed;

  public JsonRecordWriter(OutputStream outputStream) throws IOException {
    writer = new OutputStreamWriter(outputStream, "UTF-8");
    generator = ObjectMapperFactory.getOneLine().getFactory().createGenerator(writer);
  }

  @Override
  public String getEncoding() {
    return RecordEncoding.JSON1.name();
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
      LOG.warn("Error on close: {}", ex, ex);
    }
  }
}
