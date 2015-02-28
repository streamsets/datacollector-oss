/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.sdcrecord;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestJsonSdcRecordCharDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  private String createJsonSdcRecordsString() throws Exception {
    StringWriter writer = new StringWriter();
    JsonRecordWriter recordWriter = ((ContextExtensions)getContext()).createJsonRecordWriter(writer);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    recordWriter.write(record);
    record = RecordCreator.create();
    record.set(Field.create("Bye"));
    recordWriter.write(record);
    recordWriter.close();
    return writer.toString();
  }

  @Test
  public void testGetParserString() throws Exception {
    Map<String, Object> configs = JsonSdcRecordCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    CharDataParserFactory factory = new JsonSdcRecordCharDataParserFactory(getContext(), 1000, configs);
    DataParser parser = factory.getParser("id", createJsonSdcRecordsString());
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertTrue(0 < parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    Map<String, Object> configs = JsonSdcRecordCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    CharDataParserFactory factory = new JsonSdcRecordCharDataParserFactory(getContext(), 1000, configs);
    OverrunReader reader = new OverrunReader(new StringReader(createJsonSdcRecordsString()), 1000, true);
    DataParser parser = factory.getParser("id", reader, 0);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertTrue(0 < parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    String payload = createJsonSdcRecordsString();

    // find out offset of second record first
    Map<String, Object> configs = JsonSdcRecordCharDataParserFactory.registerConfigs(new HashMap<String, Object>());
    CharDataParserFactory factory = new JsonSdcRecordCharDataParserFactory(getContext(), 1000, configs);
    OverrunReader reader = new OverrunReader(new StringReader(payload), 1000, true);
    DataParser parser = factory.getParser("id", reader, 0);
    Assert.assertEquals(0, parser.getOffset());
    parser.parse();
    long offset = parser.getOffset();
    parser.close();

    factory = new JsonSdcRecordCharDataParserFactory(getContext(), 1000, configs);
    reader = new OverrunReader(new StringReader(payload), 1000, true);
    parser = factory.getParser("id", reader, offset);
    Assert.assertEquals(offset, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertTrue(offset < parser.getOffset());
    parser.close();
  }
}
