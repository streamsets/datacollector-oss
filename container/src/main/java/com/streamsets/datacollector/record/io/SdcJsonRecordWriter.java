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

public class SdcJsonRecordWriter implements RecordWriter {
  private final static Logger LOG = LoggerFactory.getLogger(SdcJsonRecordWriter.class);
  private final Writer writer;
  private final JsonGenerator generator;
  private boolean closed;

  public SdcJsonRecordWriter(OutputStream outputStream) throws IOException {
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
    generator.writeRaw('\n');
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
