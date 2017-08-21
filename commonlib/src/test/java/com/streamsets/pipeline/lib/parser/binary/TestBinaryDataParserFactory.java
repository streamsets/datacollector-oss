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
package com.streamsets.pipeline.lib.parser.binary;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class TestBinaryDataParserFactory {

  private static final String TEST_STRING_251 = "StreamSets was founded in June 2014 by business and engineering " +
    "leaders in the data integration space with a history of bringing successful products to market. We're a " +
    "team that is laser-focused on solving hard problems so our customers don't have to.";

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  private byte[] getTestBytes() throws Exception {
    return TEST_STRING_251.getBytes();
  }

  @Test
  public void testGetParser() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
                                                                                     DataParserFormat.BINARY);
    DataParserFactory factory = dataParserFactoryBuilder
        .setMaxDataLen(1000)
        .build();

    DataParser parser = factory.getParser("id", getTestBytes());
    Assert.assertEquals("0", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("251", parser.getOffset());

    Assert.assertTrue(record.has("/"));
    Assert.assertTrue(Arrays.equals(getTestBytes(), record.get("/").getValueAsByteArray()));
    Assert.assertEquals(251, record.get().getValueAsByteArray().length);

    parser.close();
  }

  @Test(expected = DataParserException.class)
  public void testParseExceedsMaxDataLength() throws Exception {
    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(),
      DataParserFormat.BINARY);
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(159)
      .build();

    DataParser parser = factory.getParser("id", getTestBytes());
    Assert.assertEquals("0", parser.getOffset());
    parser.parse();
  }
}
