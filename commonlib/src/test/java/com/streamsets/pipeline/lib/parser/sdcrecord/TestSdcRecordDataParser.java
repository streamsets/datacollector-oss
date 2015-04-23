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
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

public class TestSdcRecordDataParser {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  private byte[] createJsonSdcRecordsBytes() throws Exception {
    ByteArrayOutputStream writer = new ByteArrayOutputStream();
    RecordWriter recordWriter = ((ContextExtensions)getContext()).createRecordWriter(writer);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    recordWriter.write(record);
    record = RecordCreator.create();
    record.set(Field.create("Bye"));
    recordWriter.write(record);
    recordWriter.close();
    return writer.toByteArray();
  }

  @Test
  public void testParse() throws Exception {
    byte[] data = createJsonSdcRecordsBytes();
    InputStream reader = new ByteArrayInputStream(data);
    DataParser parser = new SdcRecordDataParser(getContext(), reader, 0, 100);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello", record.get().getValueAsString());
    long offset = parser.getOffset();
    Assert.assertTrue(0 < offset);
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Bye", record.get().getValueAsString());
    Assert.assertTrue(offset < parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    byte[] data = createJsonSdcRecordsBytes();
    InputStream reader = new ByteArrayInputStream(data);

    // find out offset of second record first
    DataParser parser = new SdcRecordDataParser(getContext(), reader, 0, 100);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello", record.get().getValueAsString());
    long offset = parser.getOffset();
    parser.close();

    reader = new ByteArrayInputStream(data);
    parser = new SdcRecordDataParser(getContext(), reader, offset, 100);
    Assert.assertEquals(offset, parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Bye", record.get().getValueAsString());
    Assert.assertTrue(offset < parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, parser.getOffset());
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    InputStream reader = new ByteArrayInputStream(createJsonSdcRecordsBytes());
    DataParser parser = new SdcRecordDataParser(getContext(), reader, 0, 100);
    parser.close();
    parser.parse();
  }

}
