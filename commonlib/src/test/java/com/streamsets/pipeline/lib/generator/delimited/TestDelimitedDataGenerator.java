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
package com.streamsets.pipeline.lib.generator.delimited;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.util.DelimitedDataConstants;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.commons.csv.CSVFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestDelimitedDataGenerator {

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);

    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.DELIMITED);
    builder.setMode(CsvMode.CSV).setMode(CsvHeader.IGNORE_HEADER).setCharset(Charset.forName("US-ASCII"));
    DataFactory dataFactory = builder.build();

    Assert.assertTrue(dataFactory instanceof DelimitedDataGeneratorFactory);
    DelimitedDataGeneratorFactory factory = (DelimitedDataGeneratorFactory)dataFactory;

    DelimitedCharDataGenerator generator = (DelimitedCharDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertEquals(CSVFormat.DEFAULT, generator.getFormat());
    Assert.assertEquals(CsvHeader.IGNORE_HEADER, generator.getHeader());
    Assert.assertEquals("header", generator.getHeaderKey());
    Assert.assertEquals("value", generator.getValueKey());

    Writer writer = factory.createWriter(new ByteArrayOutputStream());
    Assert.assertTrue(writer instanceof OutputStreamWriter);
    OutputStreamWriter outputStreamWriter = (OutputStreamWriter) writer;
    Assert.assertEquals("ASCII", outputStreamWriter.getEncoding());

    builder = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.DELIMITED);
    builder.setMode(CsvMode.CSV)
      .setMode(CsvHeader.IGNORE_HEADER)
      .setConfig(DelimitedDataGeneratorFactory.HEADER_KEY, "foo")
      .setConfig(DelimitedDataGeneratorFactory.VALUE_KEY, "bar");
    dataFactory = builder.build();
    Assert.assertTrue(dataFactory instanceof DelimitedDataGeneratorFactory);
    factory = (DelimitedDataGeneratorFactory)dataFactory;

    generator = (DelimitedCharDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertEquals("foo", generator.getHeaderKey());
    Assert.assertEquals("bar", generator.getValueKey());

  }

  @Test
  public void testGeneratorNoHeader() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", null);
    Record record = RecordCreator.create();
    List<Field> list = new ArrayList<>();
    Map<String,Field> map = new HashMap<>();
    map.put("h", Field.create("A"));
    map.put("d", Field.create("a\n\r"));
    list.add(Field.create(map));
    map.put("h", Field.create("B"));
    map.put("d", Field.create("b"));
    list.add(Field.create(map));
    record.set(Field.create(list));
    gen.write(record);
    gen.close();
    Assert.assertEquals("\"a\n\r\",b\r\n", writer.toString());
  }

  @Test
  public void testGeneratorReplaceNewLines() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", " ");
    Record record = RecordCreator.create();
    List<Field> list = new ArrayList<>();
    Map<String,Field> map = new HashMap<>();
    map.put("h", Field.create("A"));
    map.put("d", Field.create("a\n\r"));
    list.add(Field.create(map));
    map.put("h", Field.create("B"));
    map.put("d", Field.create("b"));
    list.add(Field.create(map));
    map.put("h", Field.create("C"));
    map.put("d", Field.create((String)null));
    list.add(Field.create(map));
    record.set(Field.create(list));
    gen.write(record);
    gen.close();
    Assert.assertEquals("\"a  \",b,\r\n", writer.toString());
  }

  @Test
  public void testGeneratorReplaceNewLinesWithCustomString() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", "BRNO");
    Record record = RecordCreator.create();
    List<Field> list = new ArrayList<>();
    Map<String,Field> map = new HashMap<>();
    map.put("d", Field.create("a\n\rb"));
    list.add(Field.create(map));
    record.set(Field.create(list));
    gen.write(record);
    gen.close();
    Assert.assertEquals("aBRNOBRNOb\r\n", writer.toString());
  }

  @Test
  public void testGeneratorIgnoreHeader() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.IGNORE_HEADER, "h", "d", null);
    Record record = RecordCreator.create();
    List<Field> list = new ArrayList<>();
    Map<String,Field> map = new HashMap<>();
    map.put("h", Field.create("A"));
    map.put("d", Field.create("a"));
    list.add(Field.create(map));
    map.put("h", Field.create("B"));
    map.put("d", Field.create("b"));
    list.add(Field.create(map));
    record.set(Field.create(list));
    gen.write(record);
    gen.close();
    Assert.assertEquals("a,b\r\n", writer.toString());
  }

  @Test
  public void testGeneratorWithHeader() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.WITH_HEADER, "h", "d", null);
    Record record = RecordCreator.create();
    List<Field> list = new ArrayList<>();
    Map<String,Field> map = new HashMap<>();
    map.put("h", Field.create("A"));
    map.put("d", Field.create("a"));
    list.add(Field.create(map));
    map.put("h", Field.create("B"));
    map.put("d", Field.create("b"));
    list.add(Field.create(map));
    record.set(Field.create(list));
    gen.write(record);
    map.put("d", Field.create("bb"));
    list.set(1, Field.create(map));
    record.set(Field.create(list));
    gen.write(record);
    gen.close();
    Assert.assertEquals("A,B\r\na,b\r\na,bb\r\n", writer.toString());
  }

  @Test
  public void testGeneratorListMapWithHeader() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.WITH_HEADER, "h", "d", null);

    LinkedHashMap<String, Field> linkedHashMap = new LinkedHashMap<>();
    linkedHashMap.put("firstField", Field.create("sampleValue"));
    linkedHashMap.put("secondField", Field.create(20));
    Field listMapField = Field.createListMap(linkedHashMap);
    Record record = RecordCreator.create();
    record.set(listMapField);

    gen.write(record);
    gen.close();
    Assert.assertEquals("firstField,secondField\r\nsampleValue,20\r\n", writer.toString());
  }

  @Test
  public void testGeneratorListMapIgnoreHeader() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.IGNORE_HEADER, "h", "d", null);

    LinkedHashMap<String, Field> linkedHashMap = new LinkedHashMap<>();
    linkedHashMap.put("firstField", Field.create("sampleValue"));
    linkedHashMap.put("secondField", Field.create(20));
    Field listMapField = Field.createListMap(linkedHashMap);
    Record record = RecordCreator.create();
    record.set(listMapField);

    gen.write(record);
    gen.close();
    Assert.assertEquals("sampleValue,20\r\n", writer.toString());
  }

  @Test
  public void testGeneratorListMapNoHeader() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", null);

    LinkedHashMap<String, Field> linkedHashMap = new LinkedHashMap<>();
    linkedHashMap.put("firstField", Field.create("sampleValue"));
    linkedHashMap.put("secondField", Field.create(20));
    Field listMapField = Field.createListMap(linkedHashMap);
    Record record = RecordCreator.create();
    record.set(listMapField);

    gen.write(record);
    gen.close();
    Assert.assertEquals("sampleValue,20\r\n", writer.toString());
  }

  @Test
  public void testFlush() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", null);
    Record record = RecordCreator.create();
    List<Field> list = new ArrayList<>();
    Map<String,Field> map = new HashMap<>();
    map.put("h", Field.create("A"));
    map.put("d", Field.create("a"));
    list.add(Field.create(map));
    map.put("h", Field.create("B"));
    map.put("d", Field.create("b"));
    list.add(Field.create(map));
    record.set(Field.create(list));
    gen.write(record);
    gen.flush();
    Assert.assertEquals("a,b\r\n", writer.toString());
    gen.close();
  }

  @Test
  public void testClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", null);
    Record record = RecordCreator.create();
    List<Field> list = new ArrayList<>();
    Map<String,Field> map = new HashMap<>();
    map.put("h", Field.create("A"));
    map.put("d", Field.create("a"));
    list.add(Field.create(map));
    map.put("h", Field.create("B"));
    map.put("d", Field.create("b"));
    list.add(Field.create(map));
    record.set(Field.create(list));
    gen.write(record);
    gen.close();
    gen.close();
  }

  @Test(expected = IOException.class)
  public void testWriteAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", null);
    gen.close();
    Record record = RecordCreator.create();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", null);
    gen.close();
    gen.flush();
  }

  @Test
  public void testCustomWithCustom() throws Exception {
    Stage.Context context = ContextInfoCreator.createSourceContext("", false, OnRecordError.DISCARD,
      Collections.<String>emptyList());

    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.DELIMITED);
    DataGeneratorFactory factory = builder.setMaxDataLen(100).setMode(CsvMode.CUSTOM).setMode(CsvHeader.NO_HEADER)
      .setConfig(DelimitedDataConstants.DELIMITER_CONFIG, '^')
      .setConfig(DelimitedDataConstants.ESCAPE_CONFIG, '!')
      .setConfig(DelimitedDataConstants.QUOTE_CONFIG, '\'')
      .setConfig(DelimitedDataGeneratorFactory.HEADER_KEY, "h")
      .setConfig(DelimitedDataGeneratorFactory.VALUE_KEY, "d").build();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataGenerator gen = factory.getGenerator(byteArrayOutputStream);

    Record record = RecordCreator.create();
    List<Field> list = new ArrayList<>();
    Map<String,Field> map = new HashMap<>();
    map.put("h", Field.create("A"));
    map.put("d", Field.create("a"));
    list.add(Field.create(map));
    map.put("h", Field.create("B"));
    map.put("d", Field.create("b"));
    list.add(Field.create(map));
    map.put("h", Field.create("C"));
    map.put("d", Field.create("!^"));
    list.add(Field.create(map));
    record.set(Field.create(list));

    gen.write(record);
    gen.flush();

    Assert.assertEquals("a^b^'!!^'\r\n", byteArrayOutputStream.toString());
    gen.close();
  }

}
