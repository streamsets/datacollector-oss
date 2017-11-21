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
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.StringBuilderPoolFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.util.Collections;

public class TestCEFParser {

  private static final String LOG_LINE = "CEF:0|security|threatmanager|1.0|100|detected a \\| " +
      "in message|10|src=10.0.0.1 act=blocked a \\= and \\ dst=1.1.1.1 fileName=C:\\Program Files\\test.txt port=test cs1=custom string value cs1Label=custom label";

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test
  public void testParse() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(LOG_LINE), 1000, true, false);
    DataParser parser = new CEFParser(getContext(), "id", reader, 0, 1000, true,
        getStringBuilderPool(), getStringBuilderPool());
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(LOG_LINE, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(203, Long.parseLong(parser.getOffset()));

    Assert.assertTrue(record.has("/cefVersion"));
    Assert.assertEquals(0, record.get("/cefVersion").getValueAsInteger());

    Assert.assertTrue(record.has("/name"));
    Assert.assertEquals("detected a \\| in message", record.get("/name").getValueAsString());

    Assert.assertTrue(record.has("/extensions/fileName"));
    Assert.assertEquals("C:\\Program Files\\test.txt", record.get("/extensions/fileName").getValueAsString());

    Assert.assertFalse(record.has("/extensions/cs1Label"));
    Assert.assertTrue(record.has("/extensions/'custom label'"));
    Assert.assertEquals("custom string value", record.get("/extensions/'custom label'").getValueAsString());


    parser.close();
  }

  @Test(expected = DataParserException.class)
  public void testParseNonLogLine() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] This is a log line that does not confirm to common log format"),
      1000, true, false);
    DataParser parser = new CEFParser(getContext(), "id", reader, 0, 1000, true,
        getStringBuilderPool(), getStringBuilderPool());
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  private GenericObjectPool<StringBuilder> getStringBuilderPool() {
    GenericObjectPoolConfig stringBuilderPoolConfig = new GenericObjectPoolConfig();
    stringBuilderPoolConfig.setMaxTotal(1);
    stringBuilderPoolConfig.setMinIdle(1);
    stringBuilderPoolConfig.setMaxIdle(1);
    stringBuilderPoolConfig.setBlockWhenExhausted(false);
    return new GenericObjectPool<>(new StringBuilderPoolFactory(1024), stringBuilderPoolConfig);
  }
}
