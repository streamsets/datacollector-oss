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

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvParser;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.testing.Matchers;
import org.apache.commons.csv.CSVFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.nullValue;

public class TestDelimitedCharDataParser {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testParseNoHeader() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.NO_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("0", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("4", parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseNoHeaderWithListMap() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.NO_HEADER)
        .withRecordType(CsvRecordType.LIST_MAP)
        .withMaxObjectLen(-1)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("0", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("A", record.get().getValueAsListMap().get("0").getValueAsString());
    Assert.assertEquals("B", record.get().getValueAsListMap().get("1").getValueAsString());
    Assert.assertEquals("4", parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsListMap().get("0").getValueAsString());
    Assert.assertEquals("b", record.get().getValueAsListMap().get("1").getValueAsString());
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseIgnoreHeader() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.IGNORE_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseIgnoreHeaderWithListMap() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.IGNORE_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST_MAP)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsListMap().get("0").getValueAsString());
    Assert.assertEquals("b", record.get().getValueAsListMap().get("1").getValueAsString());
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseWithHeader() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.WITH_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("header").getValueAsString());
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("header").getValueAsString());
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseWithHeaderWithListMap() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.WITH_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST_MAP)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsListMap().get("A").getValueAsString());
    Assert.assertEquals("b", record.get().getValueAsListMap().get("B").getValueAsString());
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseNoHeaderWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.NO_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 4, settings);

    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseIgnoreHeaderWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.IGNORE_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 4, settings);

    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseIgnoreHeaderWithOffset2() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b\ne,f"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.IGNORE_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 8, settings);

    Assert.assertEquals("8", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::8", record.getHeader().getSourceId());
    Assert.assertEquals("e", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("f", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("11", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseWithHeaderWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b\ne,f"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.WITH_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 8, settings);

    Assert.assertEquals("8", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::8", record.getHeader().getSourceId());
    Assert.assertEquals("e", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("header").getValueAsString());
    Assert.assertEquals("f", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("header").getValueAsString());
    Assert.assertEquals("11", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }


  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.IGNORE_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    parser.close();
    parser.parse();
  }

  @Test
  public void testParseNullConstant() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,null\nnull,B"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.NO_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(true)
        .withNullConstant("null")
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("0", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertNull(record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertNull(record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    record = parser.parse();
    Assert.assertNull(record);
    parser.close();
  }

  @Test
  public void testClRfEndOfLines() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\r\na,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.NO_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("0", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("5", parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::5", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("8", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testCrEndOfLines() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\ra,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.NO_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("0", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("4", parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseWithExtraColumnsAllowed() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("a,b,c\n1,2,3,4"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.WITH_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST_MAP)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(true)
        .withExtraColumnPrefix("_abc_")
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Record record = parser.parse();
    Assert.assertNotNull(record);
    Set<String> fieldPaths = record.getEscapedFieldPaths();
    Set<String> expectedFieldPaths = ImmutableSet.of("", "/a", "/b", "/c", "/_abc_01");
    assertTrue(fieldPaths.containsAll(expectedFieldPaths));
  }

  @Test
  public void testParseWithExtraColumnsNotAllowed() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b,c"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withFormat(CSVFormat.DEFAULT)
        .withHeader(CsvHeader.WITH_HEADER)
        .withMaxObjectLen(-1)
        .withRecordType(CsvRecordType.LIST_MAP)
        .withParseNull(false)
        .withNullConstant(null)
        .withAllowExtraColumns(false)
        .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("4", parser.getOffset());

    try {
      parser.parse();
      Assert.fail("Expected exception while parsing!");
    } catch(RecoverableDataParserException ex) {
      Record r = ex.getUnparsedRecord();
      Assert.assertNotNull(r);
      assertTrue(r.has("/columns"));
      assertTrue(r.has("/headers"));

      List<Field> headers = r.get("/headers").getValueAsList();
      Assert.assertNotNull(headers);
      Assert.assertEquals(2, headers.size());
      Assert.assertEquals("A", headers.get(0).getValueAsString());
      Assert.assertEquals("B", headers.get(1).getValueAsString());

      List<Field> columns = r.get("/columns").getValueAsList();
      Assert.assertNotNull(columns);
      Assert.assertEquals(3, columns.size());
      Assert.assertEquals("a", columns.get(0).getValueAsString());
      Assert.assertEquals("b", columns.get(1).getValueAsString());
      Assert.assertEquals("c", columns.get(2).getValueAsString());
    }
  }

  @Test
  public void testMultiCharDelimiters() throws Exception {
    // multiple character field separator
    final String multiCharFieldDelimiter = "||";
    // add a null constant and field for it in the input data
    final String nullConstant = "\\N";
    // use a strange line delimiter (not newline)
    final String lineDelimiter = "%%FOO%%";

    // craft the input data
    final String inputData = String.format(
        "field1%1$sfield2%1$sfield3%3$s" +
            "one%1$stwo%1$sthree%3$s" +
            "four%1$s%2$s%1$ssix%3$s" +
            "seven%1$seight%1$snine",
        multiCharFieldDelimiter,
        nullConstant,
        lineDelimiter
    );

    // get the index of the start of each "line", for offset checking later
    final int indexOfFirstLine = inputData.indexOf("one");
    final int indexOfSecondLine = inputData.indexOf("four");
    final int indexOfThirdLine = inputData.indexOf("seven");

    final OverrunReader reader = new OverrunReader(new StringReader(inputData), 1000, true, false);
    final DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
        .withSkipStartLines(0)
        .withMultiCharacterFieldDelimiter(multiCharFieldDelimiter)
        .withHeader(CsvHeader.WITH_HEADER)
        .withMaxObjectLen(4096)
        .withRecordType(CsvRecordType.LIST_MAP)
        .withParseNull(true)
        .withNullConstant(nullConstant)
        .withAllowExtraColumns(false)
        .withMultiCharQuoteChar('"')
        .withMultiCharEscapeChar('\\')
        .withMultiCharacterLineDelimiter(lineDelimiter)
        .build();
    final DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Assert.assertEquals("0", parser.getOffset());

    final Record record1 = parser.parse();
    final Field rootField1 = record1.get();
    assertThat(rootField1, Matchers.mapFieldWithEntry("field1", "one"));
    assertThat(rootField1, Matchers.mapFieldWithEntry("field2", "two"));
    assertThat(rootField1, Matchers.mapFieldWithEntry("field3", "three"));

    Assert.assertEquals(String.valueOf(indexOfSecondLine - indexOfFirstLine), parser.getOffset());

    final Record record2 = parser.parse();
    final Field rootField2 = record2.get();
    assertThat(rootField2, Matchers.mapFieldWithEntry("field1", "four"));
    assertThat(rootField2, Matchers.mapFieldWithEntry("field2", (String) null));
    assertThat(rootField2, Matchers.mapFieldWithEntry("field3", "six"));

    Assert.assertEquals(String.valueOf(indexOfThirdLine - indexOfFirstLine), parser.getOffset());

    final Record record3 = parser.parse();
    final Field rootField3 = record3.get();
    assertThat(rootField3, Matchers.mapFieldWithEntry("field1", "seven"));
    assertThat(rootField3, Matchers.mapFieldWithEntry("field2", "eight"));
    assertThat(rootField3, Matchers.mapFieldWithEntry("field3", "nine"));

    Assert.assertEquals(String.valueOf(inputData.length() - indexOfFirstLine), parser.getOffset());

    final Record record4 = parser.parse();
    assertThat(record4, nullValue());

    parser.close();
  }

  @Test
  public void testParserNull() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\r\nnull,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
      .withSkipStartLines(0)
      .withFormat(CSVFormat.DEFAULT)
      .withHeader(CsvHeader.WITH_HEADER)
      .withMaxObjectLen(-1)
      .withRecordType(CsvRecordType.LIST_MAP)
      .withParseNull(true)
      .withNullConstant("null")
      .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has("/A"));
    Assert.assertTrue(record.has("/B"));
    Assert.assertEquals(null, record.get("/A").getValueAsString());
    Assert.assertEquals("b", record.get("/B").getValueAsString());

    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test // See SDC-12513
  public void testParserNullSetToFalseWithNullConstant() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\r\nnull,b"), 1000, true, false);
    DelimitedDataParserSettings settings = DelimitedDataParserSettings.builder()
        .withParser(CsvParser.LEGACY_PARSER)
      .withSkipStartLines(0)
      .withFormat(CSVFormat.DEFAULT)
      .withHeader(CsvHeader.WITH_HEADER)
      .withMaxObjectLen(-1)
      .withRecordType(CsvRecordType.LIST_MAP)
      .withParseNull(false) // We should not parse nulls even thought we set the null constant below
      .withNullConstant("null")
      .build();
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, settings);

    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertTrue(record.has("/A"));
    Assert.assertTrue(record.has("/B"));
    Assert.assertEquals("null", record.get("/A").getValueAsString());
    Assert.assertEquals("b", record.get("/B").getValueAsString());

    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }
}
