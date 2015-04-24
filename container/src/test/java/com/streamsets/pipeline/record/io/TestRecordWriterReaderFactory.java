/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.record.io;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.el.ELVariables;
import com.streamsets.pipeline.record.RecordImpl;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public  class TestRecordWriterReaderFactory {

  @Test(expected = IOException.class)
  public void testInvalidMagicNumber() throws IOException {
    byte[] bytes = {64};
    InputStream is = new ByteArrayInputStream(bytes);
    RecordWriterReaderFactory.createRecordReader(is, 0, 1000);
  }

  @Test(expected = IOException.class)
  public void testUnsupportedMagicNumber() throws IOException {
    byte[] bytes = {RecordEncodingConstants.BASE_MAGIC_NUMBER & 100};
    InputStream is = new ByteArrayInputStream(bytes);
    RecordWriterReaderFactory.createRecordReader(is, 0, 1000);
  }

  private void testEncodingSelection(String encoding, byte magicNumber) throws Exception {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Map<String, Object> constants = new HashMap<>();
    if (encoding != null) {
      constants.put(RecordWriterReaderFactory.DATA_COLLECTOR_RECORD_FORMAT, encoding);
    } else {
      encoding = RecordEncoding.DEFAULT.name();
    }
    Mockito.when(context.createELVars()).thenReturn(new ELVariables(constants));
    RecordWriter writer = RecordWriterReaderFactory.createRecordWriter(context, new ByteArrayOutputStream());
    Assert.assertEquals(encoding, writer.getEncoding());
    writer.close();

    InputStream is = new ByteArrayInputStream(new byte[] { magicNumber});
    RecordReader reader = RecordWriterReaderFactory.createRecordReader(is, 0, 100);
    Assert.assertEquals(encoding, reader.getEncoding());
    reader.close();
  }

  @Test(expected = IOException.class)
  public void testInvalidEncodingSelection() throws Exception {
    testEncodingSelection("foo", RecordEncodingConstants.JSON1_MAGIC_NUMBER);
  }

  @Test
  public void testEncodingSelection() throws Exception {
    testEncodingSelection(null, RecordEncodingConstants.JSON1_MAGIC_NUMBER);
    testEncodingSelection(RecordEncoding.JSON1.name(), RecordEncodingConstants.JSON1_MAGIC_NUMBER);
    testEncodingSelection(RecordEncoding.KRYO1.name(), RecordEncodingConstants.KRYO1_MAGIC_NUMBER);
  }

  private void testRecordWriterReader(RecordEncoding encoding) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    RecordWriter writer = RecordWriterReaderFactory.createRecordWriter(encoding, os);
    RecordImpl record1 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record1.getHeader().setStagesPath("stagePath");
    record1.getHeader().setTrackingId("trackingId");
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create(new BigDecimal(1)));
    map.put("b", Field.create("Hello"));
    map.put("c", Field.create(new ArrayList<Field>()));
    record1.set(Field.create(map));
    writer.write(record1);
    RecordImpl record2 = new RecordImpl("stage2", "source2", null, null);
    record2.getHeader().setStagesPath("stagePath2");
    record2.getHeader().setTrackingId("trackingId2");
    record2.set(Field.create("Hello"));
    writer.write(record2);
    writer.close();
    byte[] bytes = os.toByteArray();
    Assert.assertEquals(encoding.getMagicNumber(), bytes[0]);
    InputStream is = new ByteArrayInputStream(bytes);
    RecordReader reader = RecordWriterReaderFactory.createRecordReader(is, 0, 1000);
    Assert.assertEquals(record1, reader.readRecord());
    Assert.assertEquals(record2, reader.readRecord());
    Assert.assertNull(reader.readRecord());
    reader.close();
  }


  private void testRecordReaderWithOffset(RecordEncoding encoding) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    RecordWriter writer = RecordWriterReaderFactory.createRecordWriter(encoding, os);
    RecordImpl record1 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record1.getHeader().setStagesPath("stagePath");
    record1.getHeader().setTrackingId("trackingId");
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create(new BigDecimal(1)));
    map.put("b", Field.create("Hello"));
    map.put("c", Field.create(new ArrayList<Field>()));
    record1.set(Field.create(map));
    writer.write(record1);
    RecordImpl record2 = new RecordImpl("stage2", "source2", null, null);
    record2.getHeader().setStagesPath("stagePath2");
    record2.getHeader().setTrackingId("trackingId2");
    record2.set(Field.create("Hello"));
    writer.write(record2);
    writer.close();
    byte[] bytes = os.toByteArray();
    Assert.assertEquals(encoding.getMagicNumber(), bytes[0]);
    InputStream is = new ByteArrayInputStream(bytes);
    RecordReader reader = RecordWriterReaderFactory.createRecordReader(is, 0, 1000);
    Assert.assertEquals(record1, reader.readRecord());
    long offset = reader.getPosition();
    reader.close();
    is = new ByteArrayInputStream(bytes);
    reader = RecordWriterReaderFactory.createRecordReader(is, offset, 1000);
    Assert.assertEquals(record2, reader.readRecord());
    Assert.assertNull(reader.readRecord());
    reader.close();
  }

  @Test
  public void testJsonRecordWriter() throws IOException {
    testRecordWriterReader(RecordEncoding.JSON1);
  }

  @Test
  public void testKryoRecordWriter() throws IOException {
    testRecordWriterReader(RecordEncoding.KRYO1);
  }

  @Test
  public void testJsonRecorWithOffset() throws IOException {
    testRecordReaderWithOffset(RecordEncoding.JSON1);
  }

  @Test
  public void testKryoRecordWithOffset() throws IOException {
    testRecordReaderWithOffset(RecordEncoding.KRYO1);
  }

}
