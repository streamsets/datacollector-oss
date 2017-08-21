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

import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.record.io.RecordEncoding;
import com.streamsets.datacollector.record.io.RecordEncodingConstants;
import com.streamsets.datacollector.record.io.RecordWriterReaderFactory;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    final Field fieldA = Field.create(new BigDecimal(1));
    fieldA.setAttribute("a_attr1", "value1");
    fieldA.setAttribute("a_attr2", "value2");
    fieldA.setAttribute("a_attr3", "value3");
    map.put("a", fieldA);
    map.put("b", Field.create("Hello"));
    map.put("c", Field.create(new ArrayList<Field>()));
    record1.set(Field.create(map));
    writer.write(record1);

    RecordImpl record2 = new RecordImpl("stage2", "source2", null, null);
    record2.getHeader().setStagesPath("stagePath2");
    record2.getHeader().setTrackingId("trackingId2");
    record2.set(Field.create("Hello"));
    writer.write(record2);

    RecordImpl record3 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record3.getHeader().setStagesPath("stagePath3");
    record3.getHeader().setTrackingId("trackingId3");
    LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
    listMap.put("a", Field.create(new BigDecimal(1)));
    listMap.put("b", Field.create("Hello"));
    listMap.put("c", Field.create(new ArrayList<Field>()));

    //Test with special characters in listMap key to test JIRA SDC-1562
    final Field fooSpaceBarField = Field.create("foo space bar");
    fooSpaceBarField.setAttribute("fooSpaceBar_attr1", "fooBar1");
    listMap.put("foo bar", fooSpaceBarField);
    listMap.put("foo[bar", Field.create("foo open bracket bar"));
    listMap.put("foo]bar", Field.create("foo close bracket bar"));
    listMap.put("foo'bar", Field.create("foo single quote bar"));
    listMap.put("foo\"bar", Field.create("foo double quote bar"));
    listMap.put("foo -+^&*()#$@!~ bar", Field.create("foo special character bar"));
    listMap.put("foo/bar", Field.create("foo slash bar"));
    listMap.put("f/oo/'ba/\'r", Field.create("foo slash quote bar"));

    //nested listMap
    LinkedHashMap<String, Field> nestedListMap = new LinkedHashMap<>();
    nestedListMap.put("foo bar", Field.create("foo space bar"));
    nestedListMap.put("foo[bar", Field.create("foo open bracket bar"));
    nestedListMap.put("foo]bar", Field.create("foo close bracket bar"));
    nestedListMap.put("foo'bar", Field.create("foo single quote bar"));
    nestedListMap.put("foo\"bar", Field.create("foo double quote bar"));
    nestedListMap.put("foo -+^&*()#$@!~ bar", Field.create("foo special character bar"));
    final Field fooSlashBarField = Field.create("foo slash bar");
    fooSlashBarField.setAttribute("fooSlashBar_attr1", "fooSlashBar1");
    fooSlashBarField.setAttribute("fooSlashBar_attr2", "fooSlashBar2");
    nestedListMap.put("foo/bar", fooSlashBarField);

    listMap.put("nestedListMap", Field.createListMap(nestedListMap));
    record3.set(Field.createListMap(listMap));
    writer.write(record3);

    writer.close();
    byte[] bytes = os.toByteArray();
    Assert.assertEquals(encoding.getMagicNumber(), bytes[0]);
    InputStream is = new ByteArrayInputStream(bytes);
    RecordReader reader = RecordWriterReaderFactory.createRecordReader(is, 0, 1000);
    Assert.assertEquals(record1, reader.readRecord());
    Assert.assertEquals(record2, reader.readRecord());
    Assert.assertEquals(record3, reader.readRecord());
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

  @Test
  public void testDecimal() throws IOException {
    // We've picked this number because if it's casted to double, then it will lead to 36.7147000000000000483...
    BigDecimal originalValue = new BigDecimal("36.7147");

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    RecordWriter writer = RecordWriterReaderFactory.createRecordWriter(RecordEncoding.JSON1, os);
    RecordImpl record1 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record1.getHeader().setStagesPath("stagePath");
    record1.getHeader().setTrackingId("trackingId");
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create(originalValue));
    record1.set(Field.create(map));
    writer.write(record1);


    InputStream is = new ByteArrayInputStream(os.toByteArray());
    RecordReader reader = RecordWriterReaderFactory.createRecordReader(is, 0, 1000);
    Record record = reader.readRecord();
    BigDecimal destinationValue = record.get("/a").getValueAsDecimal();

    Assert.assertEquals(originalValue, destinationValue);
  }

  @Test
  public void testDateTimeTypes() throws IOException {
    Date date = new Date();

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    RecordWriter writer = RecordWriterReaderFactory.createRecordWriter(RecordEncoding.JSON1, os);
    RecordImpl record1 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record1.getHeader().setStagesPath("stagePath");
    record1.getHeader().setTrackingId("trackingId");
    Map<String, Field> map = new HashMap<>();
    map.put("d", Field.create(Field.Type.DATE, date));
    map.put("dt", Field.create(Field.Type.DATETIME, date));
    map.put("t", Field.create(Field.Type.TIME, date));
    record1.set(Field.create(map));
    writer.write(record1);

    InputStream is = new ByteArrayInputStream(os.toByteArray());
    RecordReader reader = RecordWriterReaderFactory.createRecordReader(is, 0, 1000);
    Record record = reader.readRecord();

    Assert.assertTrue(record.has("/d"));
    Assert.assertEquals(Field.Type.DATE, record.get("/d").getType());
    Assert.assertEquals(date, record.get("/d").getValueAsDate());

    Assert.assertTrue(record.has("/t"));
    Assert.assertEquals(Field.Type.TIME, record.get("/t").getType());
    Assert.assertEquals(date, record.get("/t").getValueAsTime());

    Assert.assertTrue(record.has("/dt"));
    Assert.assertEquals(Field.Type.DATETIME, record.get("/dt").getType());
    Assert.assertEquals(date, record.get("/dt").getValueAsDatetime());
  }
}
