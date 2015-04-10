/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.delimited;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.data.Compression;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.data.DataFactoryBuilder;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.parser.json.JsonCharDataParserFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestDelimitedCharDataParserFactory {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  /*@Test
  public void testGetParserString() throws Exception {

    DataParserFactoryBuilder dataFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.DELIMITED);
    DataFactory dataFactory = dataFactoryBuilder
      .setMaxDataLen(1000)
      .setMode(CsvMode.CSV)
      .build();

    Assert.assertTrue(dataFactory instanceof DelimitedCharDataParserFactory);
    CharDataParserFactory charDataParserFactory = (CharDataParserFactory) dataFactory;

    DataParser parser = charDataParserFactory.getParser("id", "[\"Hello\"]\n");
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(9, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReader() throws Exception {
    Map<String, Object> configs = new HashMap<>(JsonCharDataParserFactory.CONFIGS);
    CharDataParserFactory factory = new JsonCharDataParserFactory(getContext(), 10,
                                                                  StreamingJsonParser.Mode.MULTIPLE_OBJECTS, configs);
    OverrunReader reader = new OverrunReader(new StringReader("[\"Hello\"]\n"), 1000, true);
    DataParser parser = factory.getParser("id", reader, 0);
    Assert.assertEquals(0, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(9, parser.getOffset());
    parser.close();
  }

  @Test
  public void testGetParserReaderWithOffset() throws Exception {
    Map<String, Object> configs = new HashMap<>(JsonCharDataParserFactory.CONFIGS);
    CharDataParserFactory factory = new JsonCharDataParserFactory(getContext(), 10,
                                                                  StreamingJsonParser.Mode.ARRAY_OBJECTS, configs);
    OverrunReader reader = new OverrunReader(new StringReader("[[\"Hello\"],[\"Bye\"]]\n"), 1000, true);
    DataParser parser = factory.getParser("id", reader, 10);
    Assert.assertEquals(10, parser.getOffset());
    Record record = parser.parse();
    Assert.assertTrue(record.has(""));
    Assert.assertEquals(12, parser.getOffset());
    parser.close();
  }*/
}
