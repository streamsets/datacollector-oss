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
import com.streamsets.pipeline.lib.common.SdcRecordDataFactoryUtil;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collections;

public class TestJsonSdcRecordCharDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  private byte[] createJsonSdcRecordsString() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    SdcRecordDataFactoryUtil.writeHeader(baos);
    JsonRecordWriter recordWriter = ((ContextExtensions)getContext()).createJsonRecordWriter(
      new OutputStreamWriter(baos));
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
    baos.write(new byte[] {0,0,0,4});
    JsonRecordWriter recordWriter = ((ContextExtensions)getContext()).createJsonRecordWriter(
      new OutputStreamWriter(baos));
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
    DataFactory dataFactory = dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .build();
    Assert.assertTrue(dataFactory instanceof JsonSdcRecordCharDataParserFactory);
    JsonSdcRecordCharDataParserFactory factory = (JsonSdcRecordCharDataParserFactory) dataFactory;

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
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
      DataParserFormat.SDC_RECORD);
    DataFactory dataFactory = dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .build();
    Assert.assertTrue(dataFactory instanceof JsonSdcRecordCharDataParserFactory);
    JsonSdcRecordCharDataParserFactory factory = (JsonSdcRecordCharDataParserFactory) dataFactory;

    InputStream is = new ByteArrayInputStream(createJsonSdcRecordsString());
    DataParser parser = factory.getParser("id", is, 0);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertTrue(0 < parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    byte[] payload = createJsonSdcRecordsString();

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
      DataParserFormat.SDC_RECORD);
    DataFactory dataFactory = dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .build();
    Assert.assertTrue(dataFactory instanceof JsonSdcRecordCharDataParserFactory);
    JsonSdcRecordCharDataParserFactory factory = (JsonSdcRecordCharDataParserFactory) dataFactory;

    InputStream is = new ByteArrayInputStream(payload);
    DataParser parser = factory.getParser("id", is, 0);
    Assert.assertEquals(0, parser.getOffset());
    parser.parse();
    long offset = parser.getOffset();
    parser.close();

    dataFactory = dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .build();
    Assert.assertTrue(dataFactory instanceof JsonSdcRecordCharDataParserFactory);
    factory = (JsonSdcRecordCharDataParserFactory) dataFactory;

    is = new ByteArrayInputStream(payload);
    parser = factory.getParser("id", is, offset);
    Assert.assertEquals(offset, parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has(""));
    Assert.assertTrue(offset < parser.getOffset());
    parser.close();
  }

  @Test(expected = DataParserException.class)
  public void testCreateParserUnknownFormat() throws Exception {
    byte[] payload = createJsonSdcRecordsStringWrongFormat();

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
      DataParserFormat.SDC_RECORD);
    DataFactory dataFactory = dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .build();
    Assert.assertTrue(dataFactory instanceof JsonSdcRecordCharDataParserFactory);
    JsonSdcRecordCharDataParserFactory factory = (JsonSdcRecordCharDataParserFactory) dataFactory;

    InputStream is = new ByteArrayInputStream(payload);
    factory.getParser("id", is, 0);
  }
}
