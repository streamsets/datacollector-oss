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
package com.streamsets.pipeline.lib.parser.delimited;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvParser;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.util.DelimitedDataConstants;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TestDelimitedDataParserFactory {

  @Test
  public void testCustomWithDefault() throws Exception {
    Stage.Context context = ContextInfoCreator.createSourceContext("", false, OnRecordError.DISCARD,
                                                                   Collections.<String>emptyList());
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(context, DataParserFormat.DELIMITED);
    DataParserFactory factory = builder.setMaxDataLen(100).setMode(CsvMode.CUSTOM).setMode(CsvHeader.NO_HEADER)
      .setConfig(DelimitedDataConstants.PARSER, CsvParser.LEGACY_PARSER.name())
      .setMode(CsvRecordType.LIST).build();
    DataParser parser = factory.getParser("id", "A|\"B\"|\\|");
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("|", record.get().getValueAsList().get(2).getValueAsMap().get("value").getValueAsString());
    record = parser.parse();
    Assert.assertNull(record);
    parser.close();
  }

  @Test
  public void testCustomWithCustom() throws Exception {
    Stage.Context context = ContextInfoCreator.createSourceContext("", false, OnRecordError.DISCARD,
                                                                   Collections.<String>emptyList());
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(context, DataParserFormat.DELIMITED);
    DataParserFactory factory = builder.setMaxDataLen(100).setMode(CsvMode.CUSTOM).setMode(CsvHeader.NO_HEADER)
                                       .setConfig(DelimitedDataConstants.PARSER, CsvParser.LEGACY_PARSER.name())
                                       .setMode(CsvRecordType.LIST)
                                       .setConfig(DelimitedDataConstants.DELIMITER_CONFIG, '^')
                                       .setConfig(DelimitedDataConstants.ESCAPE_CONFIG, '!')
                                       .setConfig(DelimitedDataConstants.QUOTE_CONFIG, '\'').build();
    DataParser parser = factory.getParser("id", "A^'B'^!^");
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("^", record.get().getValueAsList().get(2).getValueAsMap().get("value").getValueAsString());
    record = parser.parse();
    Assert.assertNull(record);
    parser.close();
  }

  @Test
  public void testAllowExtraColumns() throws Exception {
    Stage.Context context = ContextInfoCreator.createSourceContext("", false, OnRecordError.DISCARD, Collections.emptyList());
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(context, DataParserFormat.DELIMITED);
    DataParserFactory factory = builder
      .setMaxDataLen(100)
      .setMode(CsvMode.CSV)
      .setMode(CsvHeader.WITH_HEADER)
      .setMode(CsvRecordType.LIST_MAP)
        .setConfig(DelimitedDataConstants.PARSER, CsvParser.LEGACY_PARSER.name())
      .setConfig(DelimitedDataConstants.DELIMITER_CONFIG, '^')
      .setConfig(DelimitedDataConstants.ESCAPE_CONFIG, '!')
      .setConfig(DelimitedDataConstants.QUOTE_CONFIG, '\'')
      .setConfig(DelimitedDataConstants.ALLOW_EXTRA_COLUMNS, true)
      .build();
    DataParser parser = factory.getParser("id", "a,b,c\n" +
      "1,2,3,4\n" +
      "1,2,3,4,5\n"
    );
    // First line
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("1", record.get().getValueAsMap().get("a").getValueAsString());
    Assert.assertEquals("2", record.get().getValueAsMap().get("b").getValueAsString());
    Assert.assertEquals("3", record.get().getValueAsMap().get("c").getValueAsString());
    Assert.assertEquals("4", record.get().getValueAsMap().get("_extra_01").getValueAsString());

    // Second line
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("1", record.get().getValueAsMap().get("a").getValueAsString());
    Assert.assertEquals("2", record.get().getValueAsMap().get("b").getValueAsString());
    Assert.assertEquals("3", record.get().getValueAsMap().get("c").getValueAsString());
    Assert.assertEquals("4", record.get().getValueAsMap().get("_extra_01").getValueAsString());
    Assert.assertEquals("5", record.get().getValueAsMap().get("_extra_02").getValueAsString());

    // EOF
    record = parser.parse();
    Assert.assertNull(record);
    parser.close();
  }

  @Test
  public void testGetParserBOM() throws Exception {
    Stage.Context context = ContextInfoCreator.createSourceContext("", false,
        OnRecordError.DISCARD, Collections.<String>emptyList());

    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(context, DataParserFormat.DELIMITED);

    DataParserFactory factory = builder
        .setMaxDataLen(100)
        .setMode(CsvMode.CSV)
        .setMode(CsvHeader.NO_HEADER)
        .setMode(CsvRecordType.LIST)
        .setConfig(DelimitedDataConstants.PARSER, CsvParser.LEGACY_PARSER.name())
        .setConfig(DelimitedDataConstants.DELIMITER_CONFIG, '^')
        .setConfig(DelimitedDataConstants.ESCAPE_CONFIG, '!')
        .setConfig(DelimitedDataConstants.QUOTE_CONFIG, '\'')
        .setConfig(DelimitedDataConstants.ALLOW_EXTRA_COLUMNS, true)
        .build();

    byte[] bom = {(byte)0xef, (byte)0xbb, (byte)0xbf};
    byte[] stringBytes = "abc,def,xyz".getBytes();
    byte[] bomPlusString = new byte[bom.length + stringBytes.length];

    System.arraycopy(bom, 0, bomPlusString, 0, bom.length);
    System.arraycopy(stringBytes, 0, bomPlusString, bom.length, stringBytes.length);

    DataParser parser = factory.getParser("id", bomPlusString);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));

    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("abc", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("def", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("xyz", record.get().getValueAsList().get(2).getValueAsMap().get("value").getValueAsString());

    Assert.assertEquals(12, Long.parseLong(parser.getOffset()));
    parser.close();
  }
}
