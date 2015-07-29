/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.text;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;

public class TestTextDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testGetParserString() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.TEXT);
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(3)
      .build();

    DataParser parser = factory.getParser("id", "Hello\n".getBytes());
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has("/text"));
    Assert.assertTrue(record.has("/truncated"));
    Assert.assertEquals(6, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.TEXT);
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(3)
      .build();
    InputStream is = new ByteArrayInputStream("Hello\nBye".getBytes());
    DataParser parser = factory.getParser("id", is, 0);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has("/text"));
    Assert.assertTrue(record.has("/truncated"));
    Assert.assertEquals(6, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.TEXT);
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .build();
    InputStream is = new ByteArrayInputStream("Hello\nBye".getBytes());
    DataParser parser = factory.getParser("id", is, 6);
    Assert.assertEquals(6, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has("/text"));
    Assert.assertEquals(9, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testCharacterBaseParserMethod() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.TEXT);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .build();
    DataParser parser = factory.getParser("id", "Hello\nBye");
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has("/text"));
    Assert.assertEquals(6, Long.parseLong(parser.getOffset()));
    parser.close();
  }

}
