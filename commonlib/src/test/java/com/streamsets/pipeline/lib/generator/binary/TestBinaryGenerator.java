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
package com.streamsets.pipeline.lib.generator.binary;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
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
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestBinaryGenerator {

  private static final String TEST_STRING_255 = "StreamSets was founded in June 2014 by business and engineering " +
    "leaders in the data integration space with a history of bringing successful products to market. We’re a " +
    "team that is laser-focused on solving hard problems so our customers don’t have to.";

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
    DataFactory dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.BINARY).build();
    Assert.assertTrue(dataFactory instanceof BinaryDataGeneratorFactory);
    BinaryDataGeneratorFactory factory = (BinaryDataGeneratorFactory) dataFactory;
    BinaryDataGenerator generator = (BinaryDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertEquals("/", generator.getFieldPath());

    dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.BINARY)
      .setConfig(BinaryDataGeneratorFactory.FIELD_PATH_KEY, "/foo")
      .setCharset(Charset.forName("UTF-16"))
      .build();
    Assert.assertTrue(dataFactory instanceof BinaryDataGeneratorFactory);
    factory = (BinaryDataGeneratorFactory) dataFactory;

    generator = (BinaryDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertEquals("/foo", generator.getFieldPath());

    try {
      factory.createWriter(new ByteArrayOutputStream());
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {

    }
  }

  @Test
  public void testFlush() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream(255);
    DataGenerator gen = new BinaryDataGenerator(out, "/data");
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("data", Field.create(TEST_STRING_255.getBytes()));
    record.set(Field.create(map));
    gen.write(record);
    gen.flush();

    Assert.assertTrue(Arrays.equals(TEST_STRING_255.getBytes(), out.toByteArray()));
    gen.close();
  }

  @Test
  public void testClose() throws Exception {
    OutputStream out = new ByteArrayOutputStream(255);
    DataGenerator gen = new BinaryDataGenerator(out, "/");
    Record record = RecordCreator.create();
    record.set(Field.create(TEST_STRING_255.getBytes()));
    gen.write(record);
    gen.close();
    gen.close();
  }

  @Test(expected = IOException.class)
  public void testWriteAfterClose() throws Exception {
    OutputStream out = new ByteArrayOutputStream(255);
    DataGenerator gen = new BinaryDataGenerator(out, "/");
    Record record = RecordCreator.create();
    record.set(Field.create(TEST_STRING_255.getBytes()));
    gen.close();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    OutputStream out = new ByteArrayOutputStream(255);
    DataGenerator gen = new BinaryDataGenerator(out, "/data");
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("data", Field.create(TEST_STRING_255.getBytes()));
    record.set(Field.create(map));
    gen.close();
    gen.flush();
  }

}
