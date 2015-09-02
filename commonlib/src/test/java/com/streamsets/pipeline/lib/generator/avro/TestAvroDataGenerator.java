/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.generator.avro;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class TestAvroDataGenerator {

  private static final String AVRO_SCHEMA = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"Employee\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"name\", \"type\": \"string\"},\n"
    +" {\"name\": \"age\", \"type\": \"int\"},\n"
    +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
    +" {\"name\": \"boss\", \"type\": [\"null\", \"Employee\"]}\n"
    +"]}";

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);

    DataFactory dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.AVRO)
      .setCharset(Charset.forName("UTF-16")).setConfig(AvroDataGeneratorFactory.SCHEMA_KEY, AVRO_SCHEMA).build();
    Assert.assertTrue(dataFactory instanceof AvroDataGeneratorFactory);
    AvroDataGeneratorFactory factory = (AvroDataGeneratorFactory) dataFactory;
    AvroDataOutputStreamGenerator generator = (AvroDataOutputStreamGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertNotNull(generator);

    Writer writer = factory.createWriter(new ByteArrayOutputStream());
    Assert.assertTrue(writer instanceof OutputStreamWriter);
    OutputStreamWriter outputStreamWriter = (OutputStreamWriter) writer;
    Assert.assertEquals("UTF-16", outputStreamWriter.getEncoding());
  }

  @Test
  public void testGenerate() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(baos, AVRO_SCHEMA);
    Record record = createRecord();
    gen.write(record);
    gen.close();

    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    //reader schema must be extracted from the data file
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(null);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
      new SeekableByteArrayInput(baos.toByteArray()), reader);
    Assert.assertTrue(dataFileReader.hasNext());
    GenericRecord readRecord = dataFileReader.next();

    Assert.assertEquals("hari", readRecord.get("name").toString());
    Assert.assertEquals(3100, readRecord.get("age"));
    Assert.assertFalse(dataFileReader.hasNext());
  }

  @Test
  public void testClose() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(baos, AVRO_SCHEMA);
    Record record = createRecord();
    gen.write(record);
    gen.close();
    gen.close();
  }

  @Test(expected = IOException.class)
  public void testWriteAfterClose() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(baos, AVRO_SCHEMA);
    Record record = createRecord();
    gen.close();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(baos, AVRO_SCHEMA);
    gen.close();
    gen.flush();
  }


  @Test
  public void testAvroGeneratorNoMapping() throws IOException, DataGeneratorException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Record r = createRecord();

    DataGenerator dataGenerator = new AvroDataOutputStreamGenerator(baos, AVRO_SCHEMA);
    dataGenerator.write(r);
    dataGenerator.flush();
    dataGenerator.close();

    System.out.println(new String(baos.toByteArray()));
  }

  private Record createRecord() throws IOException {
    Map<String, Object> obj = new HashMap<>();
    obj.put("name", "hari");
    obj.put("age", 3100);
    obj.put("emails", ImmutableList.of("hari1@streamsets.com", "hari2@streamsets.com", "hari3@streamsets.com"));

    Field field = JsonUtil.jsonToField(obj);
    Record r = RecordCreator.create();
    r.set(field);
    return r;
  }

}
