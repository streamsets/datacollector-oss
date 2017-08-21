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
package com.streamsets.pipeline.lib.generator.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.io.fileref.FileRefTestUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestJsonDataGenerator {

  private Stage.Context getContext() {
    return ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
  }

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);

    DataFactory dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.JSON)
      .setMode(Mode.ARRAY_OBJECTS).setCharset(Charset.forName("UTF-16")).build();
    Assert.assertTrue(dataFactory instanceof JsonDataGeneratorFactory);
    JsonDataGeneratorFactory factory = (JsonDataGeneratorFactory) dataFactory;
    JsonCharDataGenerator generator = (JsonCharDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertEquals(true, generator.isArrayObjects());

    dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.JSON)
      .setMode(Mode.MULTIPLE_OBJECTS).setCharset(Charset.forName("UTF-16")).build();
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
    DataGenerator gen = new JsonCharDataGenerator(getContext(), writer, Mode.ARRAY_OBJECTS);
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
    DataGenerator gen = new JsonCharDataGenerator(getContext(), writer, Mode.MULTIPLE_OBJECTS);
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
    DataGenerator gen = new JsonCharDataGenerator(getContext(), writer, Mode.MULTIPLE_OBJECTS);
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
    DataGenerator gen = new JsonCharDataGenerator(getContext(), writer, Mode.MULTIPLE_OBJECTS);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.close();
    gen.close();
  }

  @Test(expected = IOException.class)
  public void testWriteAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonCharDataGenerator(getContext(), writer, Mode.MULTIPLE_OBJECTS);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonCharDataGenerator(getContext(), writer, Mode.MULTIPLE_OBJECTS);
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.flush();
  }

  @Test
  public void testGeneratorListMapType() throws Exception {
    LinkedHashMap<String, Field> linkedHashMap = new LinkedHashMap<>();
    linkedHashMap.put("firstField", Field.create("sampleValue"));
    linkedHashMap.put("secondField", Field.create(20));

    Field listMapField = Field.createListMap(linkedHashMap);
    Record record = RecordCreator.create();
    record.set(listMapField);
    StringWriter writer = new StringWriter();
    try {
      DataGenerator gen = new JsonCharDataGenerator(getContext(), writer, Mode.MULTIPLE_OBJECTS);
      gen.write(record);
      gen.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    JsonParser parser = new ObjectMapper().getFactory().createParser(writer.toString());
    Iterator<Object> it = parser.readValuesAs(Object.class);
    Assert.assertTrue(it.hasNext());
    Object obj = it.next();
    Assert.assertNotNull(obj);
    Assert.assertFalse(it.hasNext());
    Assert.assertTrue(obj instanceof LinkedHashMap);
    Assert.assertEquals(2, ((LinkedHashMap) obj).size());
  }

  @Test
  public void testFileRefTypeError() throws Exception {
    File testDir = new File("target", UUID.randomUUID().toString());
    testDir.mkdirs();
    try {
      FileRefTestUtil.writePredefinedTextToFile(testDir);
      Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
      Record record = context.createRecord("id");
      Map<String, Object> metadata = FileRefTestUtil.getFileMetadata(testDir);
      FileRef fileRef = FileRefTestUtil.getLocalFileRef(testDir, false, null, null);
      record.set(FileRefUtil.getWholeFileRecordRootField(fileRef, metadata));
      DataGenerator gen = new JsonCharDataGenerator(getContext(), new StringWriter(), Mode.MULTIPLE_OBJECTS);
      gen.write(record);
      Assert.fail("Json should not process FileRef field");
    } catch (DataGeneratorException e) {
      Assert.assertEquals(Errors.JSON_GENERATOR_01, e.getErrorCode());
    } finally {
      testDir.delete();
    }
  }
}
