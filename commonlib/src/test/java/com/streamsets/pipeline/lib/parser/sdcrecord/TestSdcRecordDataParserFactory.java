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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Collections;

public class TestSdcRecordDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  private byte[] createJsonSdcRecordsString() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RecordWriter recordWriter = ((ContextExtensions) getContext()).createRecordWriter(baos);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    recordWriter.write(record);
    record = RecordCreator.create();
    record.set(Field.create("Bye"));
    recordWriter.write(record);
    recordWriter.close();
    return baos.toByteArray();
  }

  private byte[] createJsonSdcRecordsStringWrongFormat() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(new byte[]{0, 0, 0, 4});
    RecordWriter recordWriter = ((ContextExtensions) getContext()).createRecordWriter(baos);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    recordWriter.write(record);
    record = RecordCreator.create();
    record.set(Field.create("Bye"));
    recordWriter.write(record);
    recordWriter.close();
    return baos.toByteArray();
  }

  @Test
  public void testGetParserString() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
                                                                                     DataParserFormat.SDC_RECORD);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .build();

    DataParser parser = factory.getParser("id", createJsonSdcRecordsString());
    Assert.assertEquals("0", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertTrue(0 < Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
                                                                                     DataParserFormat.SDC_RECORD);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .build();

    InputStream is = new ByteArrayInputStream(createJsonSdcRecordsString());
    DataParser parser = factory.getParser("id", is, "0");
    Assert.assertEquals("0", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertTrue(0 < Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    byte[] payload = createJsonSdcRecordsString();

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
                                                                                     DataParserFormat.SDC_RECORD);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .build();

    InputStream is = new ByteArrayInputStream(payload);
    DataParser parser = factory.getParser("id", is, "0");
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    parser.parse();
    long offset = Long.parseLong(parser.getOffset());
    parser.close();

    factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .build();

    is = new ByteArrayInputStream(payload);
    parser = factory.getParser("id", is, String.valueOf(offset));
    Assert.assertEquals(offset, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertTrue(offset < Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test(expected = DataParserException.class)
  public void testCreateParserUnknownFormat() throws Exception {
    byte[] payload = createJsonSdcRecordsStringWrongFormat();

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
                                                                                     DataParserFormat.SDC_RECORD);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .build();

    InputStream is = new ByteArrayInputStream(payload);
    factory.getParser("id", is, "0");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCharacterBaseParserMethod() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
                                                                                     DataParserFormat.SDC_RECORD);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .build();
    factory.getParser("id", new StringReader(""), 0);
  }

}
