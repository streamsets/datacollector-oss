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
package com.streamsets.pipeline.lib.generator.text;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.TextFieldMissingAction;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
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

public class TestTextDataGenerator {

  private static final String NEWLINE = "\n";

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
    DataFactory dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.TEXT).build();
    Assert.assertTrue(dataFactory instanceof TextDataGeneratorFactory);
    TextDataGeneratorFactory factory = (TextDataGeneratorFactory) dataFactory;
    TextCharDataGenerator generator = (TextCharDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertEquals("", generator.getFieldPath());
    Assert.assertEquals(false, generator.isRecordSeparatorIfNull());

    dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.TEXT)
      .setConfig(TextDataGeneratorFactory.FIELD_PATH_KEY, "/foo")
      .setConfig(TextDataGeneratorFactory.RECORD_SEPARATOR_IF_NULL_KEY, true)
      .setCharset(Charset.forName("UTF-16"))
      .build();
    Assert.assertTrue(dataFactory instanceof TextDataGeneratorFactory);
    factory = (TextDataGeneratorFactory) dataFactory;

    generator = (TextCharDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertEquals("/foo", generator.getFieldPath());
    Assert.assertEquals(true, generator.isRecordSeparatorIfNull());

    Writer writer = factory.createWriter(new ByteArrayOutputStream());
    Assert.assertTrue(writer instanceof OutputStreamWriter);
    OutputStreamWriter outputStreamWriter = (OutputStreamWriter) writer;
    Assert.assertEquals("UTF-16", outputStreamWriter.getEncoding());
  }

  @Test
  public void testGeneratorNoEmptyLines() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextCharDataGenerator(writer, "", false, NEWLINE, TextFieldMissingAction.IGNORE);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    record.set(null);
    gen.write(record);
    record.set(Field.create("Bye"));
    gen.write(record);
    gen.close();
    Assert.assertEquals("Hello" + NEWLINE + "Bye" + NEWLINE, writer.toString());
  }

  @Test
  public void testGeneratorEmptyLines() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextCharDataGenerator(writer, "", true, NEWLINE, TextFieldMissingAction.IGNORE);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    record.set(null);
    gen.write(record);
    record.set(Field.create("Bye"));
    gen.write(record);
    gen.close();
    Assert.assertEquals("Hello" + NEWLINE + NEWLINE + "Bye" + NEWLINE,
                        writer.toString());
  }

  @Test(expected = DataGeneratorException.class)
  public void testGeneratorReportErrorOnMissingField() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextCharDataGenerator(writer, "/not-there", true, NEWLINE, TextFieldMissingAction.ERROR);

    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));

    try {
      gen.write(record);
    } finally {
      gen.close();
    }
  }

  @Test
  public void testFlush() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextCharDataGenerator(writer, "", true, NEWLINE, TextFieldMissingAction.IGNORE);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.flush();
    Assert.assertEquals("Hello" + NEWLINE, writer.toString());
    gen.close();
  }

  @Test
  public void testClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextCharDataGenerator(writer, "", true, NEWLINE, TextFieldMissingAction.IGNORE);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.close();
    gen.close();
  }

  @Test(expected = IOException.class)
  public void testWriteAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextCharDataGenerator(writer, "", true, NEWLINE, TextFieldMissingAction.IGNORE);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextCharDataGenerator(writer, "", true, NEWLINE, TextFieldMissingAction.IGNORE);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.flush();
  }

  @Test
  public void testGeneratorDifferentSeparator() throws Exception {
    StringWriter writer = new StringWriter();
    String separator = NEWLINE + "^_^" + NEWLINE;
    DataGenerator gen = new TextCharDataGenerator(writer, "", false, separator, TextFieldMissingAction.IGNORE);
    Record record = RecordCreator.create();
    record.set(Field.create("First"));
    gen.write(record);
    record.set(null);
    gen.write(record);
    record.set(Field.create("Second"));
    gen.write(record);
    gen.close();
    Assert.assertEquals("First" + separator + "Second" + separator, writer.toString());
  }

  @Test
  public void testGeneratorEmptySeparator() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextCharDataGenerator(writer, "", false, "", TextFieldMissingAction.IGNORE);
    Record record = RecordCreator.create();
    record.set(Field.create("First"));
    gen.write(record);
    record.set(Field.create("Second"));
    gen.write(record);
    gen.close();
    Assert.assertEquals("FirstSecond", writer.toString());
  }

  @Test
  public void testGeneratorNullSeparator() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new TextCharDataGenerator(writer, "", false, null, TextFieldMissingAction.IGNORE);
    Record record = RecordCreator.create();
    record.set(Field.create("First"));
    gen.write(record);
    record.set(Field.create("Second"));
    gen.write(record);
    gen.close();
    Assert.assertEquals("FirstSecond", writer.toString());
  }
}
