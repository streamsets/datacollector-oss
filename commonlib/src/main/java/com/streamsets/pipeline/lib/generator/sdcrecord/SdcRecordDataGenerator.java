/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.sdcrecord;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.lib.generator.DataGenerator;

import java.io.IOException;

public class SdcRecordDataGenerator implements DataGenerator {

  private final RecordWriter writer;

  public SdcRecordDataGenerator(RecordWriter writer)
      throws IOException {
    this.writer = writer;
  }

  @Override
  public void write(Record record) throws IOException {
    writer.write(record);
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
