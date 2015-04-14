/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDelimitedDataGenerator {

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);

    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.DELIMITED);
    builder.setMode(CsvMode.CSV).setMode(CsvHeader.IGNORE_HEADER).setCharset(Charset.forName("US-ASCII"));
    DataFactory dataFactory = builder.build();

    Assert.assertTrue(dataFactory instanceof DelimitedCharDataGeneratorFactory);
    DelimitedCharDataGeneratorFactory factory = (DelimitedCharDataGeneratorFactory)dataFactory;

    DelimitedDataGenerator generator = (DelimitedDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
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
      .setConfig(DelimitedCharDataGeneratorFactory.HEADER_KEY, "foo")
      .setConfig(DelimitedCharDataGeneratorFactory.VALUE_KEY, "bar");
    dataFactory = builder.build();
    Assert.assertTrue(dataFactory instanceof DelimitedCharDataGeneratorFactory);
    factory = (DelimitedCharDataGeneratorFactory)dataFactory;

    generator = (DelimitedDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertEquals("foo", generator.getHeaderKey());
    Assert.assertEquals("bar", generator.getValueKey());

  }

  @Test
  public void testGeneratorNoHeader() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", false);
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
    DataGenerator gen = new DelimitedDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", true);
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
    Assert.assertEquals("\"a  \",b\r\n", writer.toString());
  }

  @Test
  public void testGeneratorIgnoreHeader() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.IGNORE_HEADER, "h", "d", false);
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
    DataGenerator gen = new DelimitedDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.WITH_HEADER, "h", "d", false);
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
  public void testFlush() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", false);
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
    DataGenerator gen = new DelimitedDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", false);
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
    DataGenerator gen = new DelimitedDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", false);
    gen.close();
    Record record = RecordCreator.create();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new DelimitedDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "h", "d", false);
    gen.close();
    gen.flush();
  }

}
