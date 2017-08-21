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
package com.streamsets.pipeline.lib.parser.json;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.JsonMode;
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

public class TestJsonDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testGetParserString() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setMode(JsonMode.MULTIPLE_OBJECTS)
        .build();

    DataParser parser = factory.getParser("id", "[\"Hello\"]\n".getBytes());
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(9, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setMode(JsonMode.MULTIPLE_OBJECTS)
        .build();

    InputStream is = new ByteArrayInputStream("[\"Hello\"]\n".getBytes());
    DataParser parser = factory.getParser("id", is, "0");
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(9, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setMode(JsonMode.ARRAY_OBJECTS)
        .build();

    InputStream is = new ByteArrayInputStream("[[\"Hello\"],[\"Bye\"]]\n".getBytes());
    DataParser parser = factory.getParser("id", is, "10");
    Assert.assertEquals(11, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(12, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test
  public void testCharacterBaseParserMethod() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder =
        new DataParserFactoryBuilder(getContext(), DataParserFormat.JSON);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .setMode(JsonMode.ARRAY_OBJECTS)
        .build();

    DataParser parser = factory.getParser("id", "[[\"Hello\"],[\"Bye\"]]\n");
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(9, Long.parseLong(parser.getOffset()));
    parser.close();
  }

}
