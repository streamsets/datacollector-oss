/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.record.io;

import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.record.RecordImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestRecordWriterFactory {

  @Test
  public void testJsonRecordWriter() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    RecordWriter writer = RecordWriterReaderFactory.createRecordWriter(RecordWriterReaderFactory.MAGIC_NUMBER_JSON, os);
    RecordImpl record1 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record1.getHeader().setStagesPath("stagePath");
    record1.getHeader().setTrackingId("trackingId");
    writer.write(record1);
    writer.close();
    byte[] bytes = os.toByteArray();
    Assert.assertEquals(RecordWriterReaderFactory.MAGIC_NUMBER_JSON, bytes[0]);
    InputStream is = new ByteArrayInputStream(bytes);
    RecordReader reader = RecordWriterReaderFactory.createRecordReader(is, 0, 1000);
    Assert.assertEquals(record1, reader.readRecord());
    Assert.assertNull(reader.readRecord());
  }

  @Test(expected = IOException.class)
  public void testInvalidMagicNumber() throws IOException {
    byte[] bytes = {64};
    InputStream is = new ByteArrayInputStream(bytes);
    RecordWriterReaderFactory.createRecordReader(is, 0, 1000);
  }

  @Test(expected = IOException.class)
  public void testUnsupportedMagicNumber() throws IOException {
    byte[] bytes = {RecordWriterReaderFactory.MAGIC_NUMBER_BASE & 100};
    InputStream is = new ByteArrayInputStream(bytes);
    RecordWriterReaderFactory.createRecordReader(is, 0, 1000);
  }

}
