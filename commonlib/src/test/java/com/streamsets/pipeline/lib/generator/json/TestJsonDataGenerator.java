/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

public class TestJsonDataGenerator {

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);

    DataFactory dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.JSON)
      .setMode(JsonMode.ARRAY_OBJECTS).setCharset(Charset.forName("UTF-16")).build();
    Assert.assertTrue(dataFactory instanceof JsonDataGeneratorFactory);
    JsonDataGeneratorFactory factory = (JsonDataGeneratorFactory) dataFactory;
    JsonCharDataGenerator generator = (JsonCharDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertEquals(true, generator.isArrayObjects());

    dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.JSON)
      .setMode(JsonMode.MULTIPLE_OBJECTS).setCharset(Charset.forName("UTF-16")).build();
    Assert.assertTrue(dataFactory instanceof JsonDataGeneratorFactory);
    factory = (JsonDataGeneratorFactory) dataFactory;
    generator = (JsonCharDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertEquals(false, generator.isArrayObjects());

    Writer writer = factory.createWriter(new ByteArrayOutputStream());
    Assert.assertTrue(writer instanceof OutputStreamWriter);
    OutputStreamWriter outputStreamWriter = (OutputStreamWriter) writer;
    Assert.assertEquals("UTF-16", outputStreamWriter.getEncoding());

  }
  @Test
  public void testGeneratorArrayObjects() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonCharDataGenerator(writer, JsonMode.ARRAY_OBJECTS);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    record.set(null);
    gen.write(record);
    record.set(Field.create("Bye"));
    gen.write(record);
    gen.close();
    JsonParser parser = new ObjectMapper().getFactory().createParser(writer.toString());
    Iterator<Object> it = parser.readValuesAs(Object.class);
    Assert.assertTrue(it.hasNext());
    Object obj = it.next();
    Assert.assertNotNull(obj);
    Assert.assertFalse(it.hasNext());
    Assert.assertTrue(obj instanceof List);
    Assert.assertEquals(3, ((List)obj).size());
  }

  @Test
  public void testGeneratorMultipleObjects() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonCharDataGenerator(writer, JsonMode.MULTIPLE_OBJECTS);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    record.set(null);
    gen.write(record);
    record.set(Field.create("Bye"));
    gen.write(record);
    gen.close();
    JsonParser parser = new ObjectMapper().getFactory().createParser(writer.toString());
    Iterator<Object> it = parser.readValuesAs(Object.class);
    Assert.assertTrue(it.hasNext());
    Assert.assertNotNull(it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertNull(it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertNotNull(it.next());
    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void testFlush() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonCharDataGenerator(writer, JsonMode.MULTIPLE_OBJECTS);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.flush();
    JsonParser parser = new ObjectMapper().getFactory().createParser(writer.toString());
    Iterator<Object> it = parser.readValuesAs(Object.class);
    Assert.assertTrue(it.hasNext());
    Assert.assertNotNull(it.next());
    Assert.assertFalse(it.hasNext());
    gen.close();
  }

  @Test
  public void testClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonCharDataGenerator(writer, JsonMode.MULTIPLE_OBJECTS);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.close();
    gen.close();
  }

  @Test(expected = IOException.class)
  public void testWriteAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonCharDataGenerator(writer, JsonMode.MULTIPLE_OBJECTS);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonCharDataGenerator(writer, JsonMode.MULTIPLE_OBJECTS);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.flush();
  }

}
