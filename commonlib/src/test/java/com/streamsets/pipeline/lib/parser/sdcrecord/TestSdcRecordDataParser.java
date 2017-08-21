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
package com.streamsets.pipeline.lib.parser.sdcrecord;

import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.util.SdcRecordConstants;
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

  private byte[] createJsonSdcRecordsWithSamplingBytes() throws Exception {
    ByteArrayOutputStream writer = new ByteArrayOutputStream();
    RecordWriter recordWriter = ((ContextExtensions)getContext()).createRecordWriter(writer);
    Record record = RecordCreator.create();
    record.getHeader().setAttribute(SdcRecordConstants.SDC_SAMPLED_RECORD, SdcRecordConstants.TRUE);
    record.getHeader().setAttribute(SdcRecordConstants.SDC_SAMPLED_TIME, String.valueOf(System.currentTimeMillis()));
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
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello", record.get().getValueAsString());
    long offset = Long.parseLong(parser.getOffset());
    Assert.assertTrue(0 < offset);
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Bye", record.get().getValueAsString());
    Assert.assertTrue(offset < Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    byte[] data = createJsonSdcRecordsBytes();
    InputStream reader = new ByteArrayInputStream(data);

    // find out offset of second record first
    DataParser parser = new SdcRecordDataParser(getContext(), reader, 0, 100);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Hello", record.get().getValueAsString());
    long offset = Long.parseLong(parser.getOffset());
    parser.close();

    reader = new ByteArrayInputStream(data);
    parser = new SdcRecordDataParser(getContext(), reader, offset, 100);
    Assert.assertEquals(offset, Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("Bye", record.get().getValueAsString());
    Assert.assertTrue(offset < Long.parseLong(parser.getOffset()));
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    InputStream reader = new ByteArrayInputStream(createJsonSdcRecordsBytes());
    DataParser parser = new SdcRecordDataParser(getContext(), reader, 0, 100);
    parser.close();
    parser.parse();
  }

  @Test
  public void testParseSampled() throws Exception {
    byte[] data = createJsonSdcRecordsWithSamplingBytes();
    InputStream reader = new ByteArrayInputStream(data);
    Stage.Context context = getContext();
    DataParser parser = new SdcRecordDataParser(context, reader, 0, 100);
    parser.parse();
    Timer timer = context.getTimer(SdcRecordConstants.EXTERNAL_SYSTEM_LATENCY);
    Assert.assertNotNull(timer);
  }

}
