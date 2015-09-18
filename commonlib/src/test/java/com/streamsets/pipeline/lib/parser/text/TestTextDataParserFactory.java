/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
