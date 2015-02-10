/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.ContextExt;
import com.streamsets.pipeline.api.impl.JsonRecordParser;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.record.RecordImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TestStageContextExt {

  @Test
  public void testRecordJsonSerDe() throws Exception {
    Stage.Context context = new StageContext("stage", StageType.SOURCE, false, Collections.EMPTY_LIST);
    RecordImpl record = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mime");
    record.getHeader().setStagesPath("stagePath");
    record.getHeader().setTrackingId("trackingId");

    StringWriter writer = new StringWriter();

    ObjectMapper om = ObjectMapperFactory.get();
    om.writeValue(writer, record);
    writer.close();
    StringReader reader = new StringReader(writer.toString());
    JsonRecordParser parser = ((ContextExt)context).createMultiObjectJsonRecordParser(reader, 0, 100000);
    Record newRecord = parser.readRecord();
    Assert.assertNull(parser.readRecord());
    parser.close();
    Assert.assertEquals(record, newRecord);
  }

  @Test
  public void testRecordsJsonSerDe() throws Exception {
    Stage.Context context = new StageContext("stage", StageType.SOURCE, false, Collections.EMPTY_LIST);
    RecordImpl record1 = new RecordImpl("stage", "source", new byte[] { 0, 1, 2}, "mime");
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
    om.writeValue(writer, record1);
    om.writeValue(writer, record2);
    writer.close();

    StringReader reader = new StringReader(writer.toString());
    JsonRecordParser parser = ((ContextExt)context).createMultiObjectJsonRecordParser(reader, 0, 100000);

    Record newRecord1 = parser.readRecord();
    Record newRecord2 = parser.readRecord();
    Assert.assertNotNull(newRecord1);
    Assert.assertNotNull(newRecord2);
    Assert.assertNull(parser.readRecord());
    parser.close();

    Assert.assertEquals(record1, newRecord1);
    Assert.assertEquals(record2, newRecord2);
  }

}
