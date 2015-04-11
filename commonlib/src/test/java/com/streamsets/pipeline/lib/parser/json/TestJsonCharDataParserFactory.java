/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.json;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.data.DataFactoryBuilder;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.parser.delimited.DelimitedCharDataParserFactory;
import com.streamsets.pipeline.lib.parser.text.TextCharDataParserFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestJsonCharDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testGetParserString() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    DataFactory dataFactory = dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .setMode(JsonMode.MULTIPLE_OBJECTS)
      .build();
    Assert.assertTrue(dataFactory instanceof JsonCharDataParserFactory);
    JsonCharDataParserFactory factory = (JsonCharDataParserFactory) dataFactory;

    DataParser parser = factory.getParser("id", "[\"Hello\"]\n");
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(9, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    DataFactory dataFactory = dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .setMode(JsonMode.MULTIPLE_OBJECTS)
      .build();
    Assert.assertTrue(dataFactory instanceof JsonCharDataParserFactory);
    JsonCharDataParserFactory factory = (JsonCharDataParserFactory) dataFactory;

    InputStream is = new ByteArrayInputStream("[\"Hello\"]\n".getBytes());
    DataParser parser = factory.getParser("id", is, 0);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(9, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    DataFactory dataFactory = dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .setMode(JsonMode.ARRAY_OBJECTS)
      .build();
    Assert.assertTrue(dataFactory instanceof JsonCharDataParserFactory);
    JsonCharDataParserFactory factory = (JsonCharDataParserFactory) dataFactory;

    InputStream is = new ByteArrayInputStream("[[\"Hello\"],[\"Bye\"]]\n".getBytes());
    DataParser parser = factory.getParser("id", is, 10);
    Assert.assertEquals(10, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(12, parser.getOffset());
    parser.close();
  }
}
