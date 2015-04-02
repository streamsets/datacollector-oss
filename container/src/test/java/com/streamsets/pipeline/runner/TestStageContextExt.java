/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordReader;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.RecordJson;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestStageContextExt {

  @Test
  public void testRecordJsonReader() throws Exception {
    Stage.Context context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                             Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap());
    RecordImpl record = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record.getHeader().setStagesPath("stagePath");
    record.getHeader().setTrackingId("trackingId");

    StringWriter writer = new StringWriter();

    ObjectMapper om = ObjectMapperFactory.get();
    om.writeValue(writer, BeanHelper.wrapRecord(record));
    writer.close();
    StringReader reader = new StringReader(writer.toString());
    JsonRecordReader rr = ((ContextExtensions)context).createJsonRecordReader(reader, 0, 100000);
    Record newRecord = rr.readRecord();
    Assert.assertNull(rr.readRecord());
    rr.close();
    Assert.assertEquals(record, newRecord);
  }

  @Test
  public void testRecordsJsonReader() throws Exception {
    Stage.Context context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                             Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap());
    RecordImpl record1 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record1.getHeader().setStagesPath("stagePath");
    record1.getHeader().setTrackingId("trackingId");

    Record record2 = record1.clone();
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create("FOO"));
    map.put("b", Field.createDate(new Date()));
    map.put("c", Field.create(new ArrayList<Field>()));
    record2.set(Field.create(map));

    StringWriter writer = new StringWriter();
    ObjectMapper om = ObjectMapperFactory.get();
    om.writeValue(writer, BeanHelper.wrapRecord(record1));
    om.writeValue(writer, BeanHelper.wrapRecord(record2));
    writer.close();

    StringReader reader = new StringReader(writer.toString());
    JsonRecordReader rr = ((ContextExtensions)context).createJsonRecordReader(reader, 0, 100000);

    Record newRecord1 = rr.readRecord();
    Record newRecord2 = rr.readRecord();
    Assert.assertNotNull(newRecord1);
    Assert.assertNotNull(newRecord2);
    Assert.assertNull(rr.readRecord());
    rr.close();

    Assert.assertEquals(record1, newRecord1);
    Assert.assertEquals(record2, newRecord2);
  }


  @Test
  public void testRecordJsonWriter() throws Exception {
    Stage.Context context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                             Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap());
    RecordImpl record = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record.getHeader().setStagesPath("stagePath");
    record.getHeader().setTrackingId("trackingId");

    StringWriter writer = new StringWriter();
    JsonRecordWriter rw = ((ContextExtensions)context).createJsonRecordWriter(writer);

    rw.write(record);
    rw.close();

    RecordJson newRecordJson = ObjectMapperFactory.get().readValue(writer.toString(),
      RecordJson.class);
    Assert.assertEquals(record, BeanHelper.unwrapRecord(newRecordJson));
  }

  @Test
  public void testRecordsJsonWriter() throws Exception {
    Stage.Context context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                             Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap());
    RecordImpl record1 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record1.getHeader().setStagesPath("stagePath");
    record1.getHeader().setTrackingId("trackingId");

    Record record2 = record1.clone();
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create("FOO"));
    map.put("b", Field.createDate(new Date()));
    map.put("c", Field.create(new ArrayList<Field>()));
    record2.set(Field.create(map));

    StringWriter writer = new StringWriter();
    JsonRecordWriter rw = ((ContextExtensions)context).createJsonRecordWriter(writer);

    rw.write(record1);
    rw.flush();
    rw.write(record2);
    rw.close();

    Iterator<RecordJson> it = ObjectMapperFactory.get().getFactory().
      createParser(writer.toString()).readValuesAs(RecordJson.class);
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(record1, BeanHelper.unwrapRecord(it.next()));
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(record2, BeanHelper.unwrapRecord(it.next()));
    Assert.assertFalse(it.hasNext());

  }

  @Test
  public void testRecordsJsonWriterReader() throws Exception {
    Stage.Context context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                             Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap());
    RecordImpl record1 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record1.getHeader().setStagesPath("stagePath");
    record1.getHeader().setTrackingId("trackingId");
    record1.getHeader().setAttribute("attr", "ATTR");
    record1.getHeader().setSourceRecord(record1);
    RecordImpl record2 = record1.clone();
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create("FOO"));
    map.put("b", Field.createDate(new Date()));
    map.put("c", Field.create(new ArrayList<Field>()));
    record2.set(Field.create(map));

    StringWriter writer = new StringWriter();
    JsonRecordWriter rw = ((ContextExtensions)context).createJsonRecordWriter(writer);

    rw.write(record1);
    rw.flush();
    rw.write(record2);
    rw.close();

    JsonRecordReader rr = ((ContextExtensions)context).createJsonRecordReader(new StringReader(writer.toString()), 0 ,0);
    Record newRecord1 = rr.readRecord();
    Assert.assertNotNull(newRecord1);
    Record newRecord2 = rr.readRecord();
    Assert.assertNotNull(newRecord2);
    Assert.assertNull(rr.readRecord());
    rr.close();
    Assert.assertEquals(record1, newRecord1);
    Assert.assertEquals(record2, newRecord2);
  }

  @Test
  public void testJsonWriterReaderRecordsWithByteArray() throws Exception {
    Stage.Context context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                             Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap());
    RecordImpl record1 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mode");
    record1.getHeader().setStagesPath("stagePath");
    record1.getHeader().setTrackingId("trackingId");
    record1.getHeader().setAttribute("attr", "ATTR");
    record1.getHeader().setSourceRecord(record1);
    RecordImpl record2 = record1.clone();
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create("Streamsets Inc, San Francisco".getBytes()));
    map.put("b", Field.create(Field.Type.BYTE_ARRAY, null));
    record2.set(Field.create(map));

    StringWriter writer = new StringWriter();
    JsonRecordWriter rw = ((ContextExtensions)context).createJsonRecordWriter(writer);

    rw.write(record1);
    rw.flush();
    rw.write(record2);
    rw.close();

    JsonRecordReader rr = ((ContextExtensions)context).createJsonRecordReader(new StringReader(writer.toString()), 0 ,0);
    Record newRecord1 = rr.readRecord();
    Assert.assertNotNull(newRecord1);
    Record newRecord2 = rr.readRecord();
    Assert.assertNotNull(newRecord2);
    Assert.assertNull(rr.readRecord());
    rr.close();
    Assert.assertEquals(record1, newRecord1);
    //Comparing records using equals does not work for byte[] fields, there is a JIRA to fix this -SDC 171

    Assert.assertTrue(Arrays.equals(record2.get("/a").getValueAsByteArray(), newRecord2.get("/a").getValueAsByteArray()));
    Assert.assertNull(newRecord2.get("/b").getValueAsByteArray());
  }

}
