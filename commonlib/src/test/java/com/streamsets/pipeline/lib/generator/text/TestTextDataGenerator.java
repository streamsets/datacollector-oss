/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.text;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class TestTextDataGenerator {

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
    Map<String, Object> configs = new HashMap<>();
    TextCharDataGeneratorFactory.registerConfigs(configs);
    TextCharDataGeneratorFactory factory = new TextCharDataGeneratorFactory(context, configs);
    TextDataGenerator generator = (TextDataGenerator) factory.getGenerator(new StringWriter());
    Assert.assertEquals("", generator.getFieldPath());
    Assert.assertEquals(false, generator.isEmptyLineIfNull());

    configs.put(TextCharDataGeneratorFactory.FIELD_PATH_KEY, "/foo");
    configs.put(TextCharDataGeneratorFactory.EMPTY_LINE_IF_NULL_KEY, true);
    factory = new TextCharDataGeneratorFactory(context, configs);
    generator = (TextDataGenerator) factory.getGenerator(new StringWriter());
    Assert.assertEquals("/foo", generator.getFieldPath());
    Assert.assertEquals(true, generator.isEmptyLineIfNull());
  }

  @Test
  public void testGeneratorNoEmptyLines() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextDataGenerator(writer, "", false);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    record.set(null);
    gen.write(record);
    record.set(Field.create("Bye"));
    gen.write(record);
    gen.close();
    Assert.assertEquals("Hello" + TextDataGenerator.EOL + "Bye" + TextDataGenerator.EOL, writer.toString());
  }

  @Test
  public void testGeneratorEmptyLines() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextDataGenerator(writer, "", true);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    record.set(null);
    gen.write(record);
    record.set(Field.create("Bye"));
    gen.write(record);
    gen.close();
    Assert.assertEquals("Hello" + TextDataGenerator.EOL + TextDataGenerator.EOL + "Bye" + TextDataGenerator.EOL,
                        writer.toString());
  }

  @Test
  public void testFlush() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextDataGenerator(writer, "", true);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.flush();
    Assert.assertEquals("Hello" + TextDataGenerator.EOL, writer.toString());
    gen.close();
  }

  @Test
  public void testClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextDataGenerator(writer, "", true);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.close();
    gen.close();
  }

  @Test(expected = IOException.class)
  public void testWriteAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextDataGenerator(writer, "", true);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextDataGenerator(writer, "", true);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.flush();
  }

}
