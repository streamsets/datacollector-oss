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
package com.streamsets.pipeline.lib.parser.text;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.parser.WrapperDataParserFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.NoSuchElementException;

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
    DataParser parser = factory.getParser("id", is, "0");
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
    DataParser parser = factory.getParser("id", is, "6");
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

  @Test
  public void testCustomDelimiterSettingWithEscapeCharacters() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.TEXT);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setConfig(TextDataParserFactory.USE_CUSTOM_DELIMITER_KEY, true)
        .setConfig(TextDataParserFactory.CUSTOM_DELIMITER_KEY, "\\r\\n")
        .build();
    try (DataParser parser = factory.getParser("id", "Hello\r\nBye")) {
      Assert.assertEquals(0, Long.parseLong(parser.getOffset()));

      Record record = parser.parse();
      Assert.assertNotNull(record);
      Assert.assertTrue(record.has("/text"));
      Assert.assertEquals("Hello", record.get("/text").getValueAsString());

      record = parser.parse();
      Assert.assertNotNull(record);
      Assert.assertTrue(record.has("/text"));
      Assert.assertEquals("Bye", record.get("/text").getValueAsString());

      record = parser.parse();
      Assert.assertNull(record);
    }
  }

  @Test
  public void testCustomDelimiterSettingWithDoubleEscapeCharacters() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.TEXT);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setConfig(TextDataParserFactory.USE_CUSTOM_DELIMITER_KEY, true)
        .setConfig(TextDataParserFactory.CUSTOM_DELIMITER_KEY, "\\\\r\\\\n")
        .build();
    try (DataParser parser = factory.getParser("id", "Hello\\r\\nBye")) {
      Record record = parser.parse();
      Assert.assertNotNull(record);
      Assert.assertTrue(record.has("/text"));
      Assert.assertEquals("Hello", record.get("/text").getValueAsString());

      record = parser.parse();
      Assert.assertNotNull(record);
      Assert.assertTrue(record.has("/text"));
      Assert.assertEquals("Bye", record.get("/text").getValueAsString());

      record = parser.parse();
      Assert.assertNull(record);
    }
  }

  @Test
  public void testParserFactoryStringBuilderPool() throws Exception {

    // Parser with default string builder pool config
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.TEXT);
    WrapperDataParserFactory factory = (WrapperDataParserFactory) dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .setConfig(TextDataParserFactory.USE_CUSTOM_DELIMITER_KEY, true)
      .setConfig(TextDataParserFactory.CUSTOM_DELIMITER_KEY, "\\\\r\\\\n")
      .build();

    TextDataParserFactory textDataParserFactory = (TextDataParserFactory) factory.getFactory();
    GenericObjectPool<StringBuilder> stringBuilderPool = textDataParserFactory.getStringBuilderPool();
    Assert.assertNotNull(stringBuilderPool);
    Assert.assertEquals(DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE, stringBuilderPool.getMaxIdle());
    Assert.assertEquals(1, stringBuilderPool.getMinIdle());
    Assert.assertEquals(DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE, stringBuilderPool.getMaxTotal());
    Assert.assertEquals(0, stringBuilderPool.getNumIdle());
    Assert.assertEquals(0, stringBuilderPool.getNumActive());

    DataParser parser = factory.getParser("id", "Hello\\r\\nBye");
    DataParser parser2 = factory.getParser("id", "Hello\\r\\nBye");
    DataParser parser3 = factory.getParser("id", "Hello\\r\\nBye");

    Assert.assertEquals(0, stringBuilderPool.getNumIdle());
    Assert.assertEquals(3, stringBuilderPool.getNumActive());

    parser.close();
    parser2.close();
    parser3.close();

    Assert.assertEquals(3, stringBuilderPool.getNumIdle());
    Assert.assertEquals(0, stringBuilderPool.getNumActive());
  }

  @Test
  public void testStringBuilderPoolException() throws Exception {

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.TEXT);
    WrapperDataParserFactory factory = (WrapperDataParserFactory) dataParserFactoryBuilder
      .setMaxDataLen(1000)
      .setConfig(TextDataParserFactory.USE_CUSTOM_DELIMITER_KEY, true)
      .setConfig(TextDataParserFactory.CUSTOM_DELIMITER_KEY, "\\\\r\\\\n")
      .build();

    TextDataParserFactory textDataParserFactory = (TextDataParserFactory) factory.getFactory();
    GenericObjectPool<StringBuilder> stringBuilderPool = textDataParserFactory.getStringBuilderPool();
    Assert.assertNotNull(stringBuilderPool);
    Assert.assertEquals(DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE, stringBuilderPool.getMaxIdle());
    Assert.assertEquals(1, stringBuilderPool.getMinIdle());
    Assert.assertEquals(DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE, stringBuilderPool.getMaxTotal());
    Assert.assertEquals(0, stringBuilderPool.getNumIdle());
    Assert.assertEquals(0, stringBuilderPool.getNumActive());

    for (int i = 0; i < DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE; ++i) {
      factory.getParser("id" + i, "Hello\\r\\nBye");
    }

    Assert.assertEquals(0, stringBuilderPool.getNumIdle());
    Assert.assertEquals(50, stringBuilderPool.getNumActive());

    try {
      factory.getParser("id" + (DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE + 1), "Hello\\r\\nBye");
      Assert.fail("Expected IOException which wraps NoSuchElementException since pool is empty");
    } catch (DataParserException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
      Assert.assertTrue(e.getCause().getCause() instanceof NoSuchElementException);
    }

  }
}
