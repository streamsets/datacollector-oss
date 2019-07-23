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
package com.streamsets.pipeline.lib.generator.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.lib.util.CommonError;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.COMPRESSION_CODEC_DEFAULT;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.DEFAULT_VALUES_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_KEY;

public class TestAvroDataGenerator {

  private static final String AVRO_SCHEMA = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"Employee\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"name\", \"type\": \"string\"},\n"
    +" {\"name\": \"age\", \"type\": \"int\"},\n"
    +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
    +" {\"name\": \"boss\", \"type\": [\"null\", \"Employee\"], \"default\" : null}\n"
    +"]}";

  private static final Schema SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA);

  private static final String DECIMAL_AVRO_SCHEMA = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"WithDecimal\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"decimal\", \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 2, \"scale\": 1}"
    +"]}";
  private static final Schema DECIMAL_SCHEMA = new Schema.Parser().parse(DECIMAL_AVRO_SCHEMA);

  private static final String DATE_AVRO_SCHEMA = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"WithDate\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"d\", \"type\": \"int\", \"logicalType\": \"date\"}"
    +"]}";
  private static final Schema DATE_SCHEMA = new Schema.Parser().parse(DATE_AVRO_SCHEMA);


  private static final String INVALID_SCHEMA = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"Employee\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"name\", \"type\": \"string\"},\n"
    +" {\"name\": \"age\"},\n"
    +"]}";

  private static final String INVALID_SCHEMA_DEFAULTS = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"Employee\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"name\", \"type\": \"string\"},\n"
    +" {\"name\": \"age\", \"type\" : [\"null\", \"string\"], \"default\" : \"hello\"},\n"
    +"]}";

  private static final String STRING_UNION_SCHEMA = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"stringunion\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"string\", \"type\" : [\"null\", \"string\"]}\n"
    +"]}";

  private static final String RECORD_SCHEMA = "{\n"
    +"  \"type\": \"record\",\n"
    +"  \"name\": \"Employee\",\n"
    +"  \"fields\": [\n"
    +"    {\"name\": \"name\", \"type\": \"string\", \"default\": \"Hello\"},\n"
    +"    {\"name\": \"age\", \"type\": \"int\", \"default\": 25},\n"
    +"    {\"name\": \"resident\", \"type\": \"boolean\", \"default\": false},\n"
    +"    {\"name\": \"enum\",\"type\":{\"type\":\"enum\",\"name\":\"Suit\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]}, \"default\": \"DIAMONDS\"},\n"
    +"    {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}, \"default\" : [\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]},\n"
    +"    {\"name\": \"phones\", \"type\": {\"type\": \"map\", \"values\": \"long\"}, \"default\" : {\"home\" : 8675309, \"mobile\" : 8675308}},\n"
    +"    {\"name\": \"boss\", \"type\": [\"null\", \"Employee\"], \"default\" : null}\n"
    +"  ]" +
    " }";

  private static final String NESTED_RECORD_SCHEMA = "{\n"
      +" \"type\": \"record\",\n"
      +" \"name\": \"xyz\",\n"
      +" \"namespace\": \"nested_record\",\n"
      +" \"doc\": \"\",\n"
      +" \"fields\": [\n"
      +"   {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": null}, \n"
      +"   {\"name\": \"parts\", \"type\": [\"null\", {\n"
      +"     \"type\": \"record\",\n"
      +"     \"name\": \"parts\",\n"
      +"     \"namespace\": \"\",\n"
      +"     \"fields\": [\n"
      +"       {\"name\": \"required\", \"type\": [\"null\", {\n"
      +"         \"type\": \"record\",\n"
      +"         \"name\": \"parts\",\n"
      +"         \"namespace\": \"required\",\n"
      +"         \"fields\": [\n"
      +"           {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      +"           {\"name\": \"params\", \"type\": [\"null\", {\n"
      +"             \"type\": \"record\",\n"
      +"             \"name\": \"parts\",\n"
      +"             \"namespace\": \"params.required\",\n"
      +"             \"fields\": [\n"
      +"               {\"name\": \"size\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
      +"             ]\n"
      +"           }],\n"
      +"           \"default\": null\n"
      +"     }]\n"
      +"    }],\n"
      +"    \"default\": null\n"
      +"   },\n"
      +"   {\"name\": \"optional\", \"type\": [\"null\", {\n"
      +"     \"type\": \"record\",\n"
      +"     \"name\": \"parts\",\n"
      +"     \"namespace\": \"optional\",\n"
      +"     \"fields\": [\n"
      +"       {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      +"       {\"name\": \"params\", \"type\": [\"null\", {\n"
      +"         \"type\": \"record\",\n"
      +"         \"name\": \"parts\",\n"
      +"         \"namespace\": \"params.optional\",\n"
      +"         \"fields\": [\n"
      +"           {\"name\": \"color\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
      +"         ]\n"
      +"       }],\n"
      +"       \"default\": null\n"
      +"   }]\n"
      +"   }],\n"
      +"   \"default\": null\n"
      +" }]\n"
      +"  }],\n"
      +"  \"default\": null\n"
      +" }]\n"
      +"}";

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
    DataFactory dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.AVRO)
      .setCharset(Charset.forName("UTF-16")).setConfig(SCHEMA_KEY, AVRO_SCHEMA).build();
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
    DataGenerator gen = new AvroDataOutputStreamGenerator(
      false,
      baos,
      COMPRESSION_CODEC_DEFAULT,
      SCHEMA,
      AvroTypeUtil.getDefaultValuesFromSchema(SCHEMA, new HashSet<String>()),
      null,
      null,
      0
    );
    Record record = createRecord();
    gen.write(record);
    gen.close();

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

  private void testGenerateCompressed(String codecName) throws Exception {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
        false,
        baos,
        codecName,
        SCHEMA,
        AvroTypeUtil.getDefaultValuesFromSchema(SCHEMA, new HashSet<String>()),
        null,
        null,
        0
    );
    Record record = createRecord();
    gen.write(record);
    gen.close();

    //reader schema must be extracted from the data file
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(null);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
        new SeekableByteArrayInput(baos.toByteArray()), reader);
    Assert.assertEquals(codecName, dataFileReader.getMetaString("avro.codec"));
    Assert.assertTrue(dataFileReader.hasNext());
    GenericRecord readRecord = dataFileReader.next();

    Assert.assertEquals("hari", readRecord.get("name").toString());
    Assert.assertEquals(3100, readRecord.get("age"));
    Assert.assertFalse(dataFileReader.hasNext());
  }

  @Test
  public void testGenerateSnappy() throws Exception {
    testGenerateCompressed("snappy");
  }

  @Test
  public void testGenerateDeflate() throws Exception {
    testGenerateCompressed("deflate");
  }

  @Test
  public void testGenerateBzip2() throws Exception {
    testGenerateCompressed("bzip2");
  }

  @Test
  public void testClose() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
      false,
      baos,
      COMPRESSION_CODEC_DEFAULT,
      SCHEMA,
      AvroTypeUtil.getDefaultValuesFromSchema(SCHEMA, new HashSet<String>()),
      null,
      null,
      0
    );
    Record record = createRecord();
    gen.write(record);
    gen.close();
    gen.close();
  }

  @Test(expected = IOException.class)
  public void testWriteAfterClose() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
        false,
        baos,
        COMPRESSION_CODEC_DEFAULT,
        SCHEMA,
        new HashMap<String, Object>(),
        null,
        null,
        0
    );
    Record record = createRecord();
    gen.close();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
        false,
        baos,
        COMPRESSION_CODEC_DEFAULT,
        SCHEMA,
        new HashMap<String, Object>(),
        null,
        null,
        0
    );
    gen.close();
    gen.flush();
  }


  @Test
  public void testAvroGeneratorNoMapping() throws IOException, DataGeneratorException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Record r = createRecord();

    DataGenerator dataGenerator = new AvroDataOutputStreamGenerator(
        false,
        baos,
        COMPRESSION_CODEC_DEFAULT,
        SCHEMA,
        AvroTypeUtil.getDefaultValuesFromSchema(SCHEMA, new HashSet<String>()),
        null,
        null,
        0

    );
    dataGenerator.write(r);
    dataGenerator.flush();
    dataGenerator.close();

    System.out.println(new String(baos.toByteArray()));
  }

  @Test
  public void testAvroGeneratorListMapType() throws Exception {
    LinkedHashMap<String, Field> linkedHashMap = new LinkedHashMap<>();
    linkedHashMap.put("name", Field.create("Jon Natkins"));
    linkedHashMap.put("age", Field.create(29));
    linkedHashMap.put("emails", Field.create(ImmutableList.of(Field.create("natty@streamsets.com"))));
    linkedHashMap.put("boss", Field.create(Field.Type.MAP, null));
    Field listMapField = Field.createListMap(linkedHashMap);
    Record record = RecordCreator.create();
    record.set(listMapField);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
        false,
        baos,
        COMPRESSION_CODEC_DEFAULT,
        SCHEMA,
        new HashMap<String, Object>(),
        null,
        null,
        0
    );
    gen.write(record);
    gen.close();

    //reader schema must be extracted from the data file
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(null);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
        new SeekableByteArrayInput(baos.toByteArray()), reader);
    Assert.assertTrue(dataFileReader.hasNext());
    GenericRecord readRecord = dataFileReader.next();

    Assert.assertEquals("Jon Natkins", readRecord.get("name").toString());
    Assert.assertEquals(29, readRecord.get("age"));
    Assert.assertFalse(dataFileReader.hasNext());
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

  @SuppressWarnings("unchecked")
  @Test
  public void testGenerateWithDefaults() throws Exception {

    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);

    DataFactory dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.AVRO)
      .setCharset(Charset.forName("UTF-16"))
      .setConfig(SCHEMA_KEY, RECORD_SCHEMA)
      .setConfig(
          DEFAULT_VALUES_KEY,
          AvroTypeUtil.getDefaultValuesFromSchema(new Schema.Parser().parse(RECORD_SCHEMA), new HashSet<String>())
      )
      .build();
    Assert.assertTrue(dataFactory instanceof AvroDataGeneratorFactory);
    AvroDataGeneratorFactory factory = (AvroDataGeneratorFactory) dataFactory;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    AvroDataOutputStreamGenerator gen = (AvroDataOutputStreamGenerator) factory.getGenerator(baos);
    Assert.assertNotNull(gen);

    Record record = RecordCreator.create();
    Map<String, Field> employee = new HashMap<>();
    record.set(Field.create(employee));

    gen.write(record);
    gen.close();

    // reader schema must be extracted from the data file
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(null);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
      new SeekableByteArrayInput(baos.toByteArray()), reader);
    Assert.assertTrue(dataFileReader.hasNext());
    GenericRecord result = dataFileReader.next();

    Assert.assertEquals("Hello", result.get("name").toString());
    Assert.assertEquals(25, result.get("age"));
    Assert.assertEquals(false, result.get("resident"));
    Assert.assertEquals("DIAMONDS", result.get("enum").toString());

    List<Utf8> emails = (List<Utf8>) result.get("emails");
    Assert.assertEquals(4, emails.size());
    Assert.assertEquals("SPADES", emails.get(0).toString());
    Assert.assertEquals("HEARTS", emails.get(1).toString());
    Assert.assertEquals("DIAMONDS", emails.get(2).toString());
    Assert.assertEquals("CLUBS", emails.get(3).toString());

    Assert.assertEquals(null, result.get("boss"));

    Map<Utf8, Object> phones = (Map<Utf8, Object>) result.get("phones");
    Assert.assertEquals(8675309, (long)phones.get(new Utf8("home")));
    Assert.assertEquals(8675308, (long)phones.get(new Utf8("mobile")));
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testGenerateWithNestedRecordsAndDefaults() throws Exception {

    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);

    DataFactory dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.AVRO)
        .setCharset(Charset.forName("UTF-16"))
        .setConfig(SCHEMA_KEY, NESTED_RECORD_SCHEMA)
        .setConfig(
            DEFAULT_VALUES_KEY,
            AvroTypeUtil.getDefaultValuesFromSchema(new Schema.Parser().parse(NESTED_RECORD_SCHEMA), new HashSet<String>())
        )
        .build();
    Assert.assertTrue(dataFactory instanceof AvroDataGeneratorFactory);
    AvroDataGeneratorFactory factory = (AvroDataGeneratorFactory) dataFactory;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    AvroDataOutputStreamGenerator gen = (AvroDataOutputStreamGenerator) factory.getGenerator(baos);
    Assert.assertNotNull(gen);

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.builder()
        .put("name", Field.create(Field.Type.STRING, "my_name"))
        .put("parts", Field.create(Field.Type.MAP, ImmutableMap.builder()
            .put("required", Field.create(Field.Type.MAP, ImmutableMap.builder()
                .put("name", Field.create(Field.Type.STRING, "nothing"))
                .put("params", Field.create(Field.Type.MAP, ImmutableMap.builder()
                    .put("size", Field.create(Field.Type.STRING, "size"))
                    .put("randomField", Field.create(Field.Type.STRING, "random"))
                    .build()))
                .build()))
            .put("optional", Field.create(Field.Type.MAP, ImmutableMap.builder()
                .put("params", Field.create(Field.Type.MAP, ImmutableMap.builder()
                    .put("color", Field.create(Field.Type.STRING, "green"))
                    .put("randomField", Field.create(Field.Type.STRING, "random"))
                    .build()))
                .build()))
            .build()))
        .build()));
    gen.write(record);
    gen.close();

    // reader schema must be extracted from the data file
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(null);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
        new SeekableByteArrayInput(baos.toByteArray()), reader);
    Assert.assertTrue(dataFileReader.hasNext());
    GenericRecord result = dataFileReader.next();

    Assert.assertEquals("my_name", result.get("name").toString());

    GenericRecord parts = (GenericRecord) result.get("parts");
    GenericRecord required = (GenericRecord) parts.get("required");
    Assert.assertEquals("nothing", required.get("name").toString());
    GenericRecord params1 = (GenericRecord) required.get("params");
    Assert.assertEquals("size", params1.get("size").toString());
    Assert.assertNull(params1.get("color"));
    Assert.assertNull(params1.get("randomField"));
    GenericRecord optional = (GenericRecord) parts.get("optional");
    Assert.assertNull(optional.get("name"));
    GenericRecord params2 = (GenericRecord) optional.get("params");
    Assert.assertNull(params2.get("size"));
    Assert.assertEquals("green", params2.get("color").toString());
    Assert.assertNull(params2.get("randomField"));
  }

  @Test
  public void testFactoryInvalidSchema() throws Exception {
    // schema used is invalid as it does not define type for field "age"
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
    try {
      new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.AVRO)
        .setCharset(Charset.forName("UTF-16"))
        .setConfig(SCHEMA_KEY, INVALID_SCHEMA)
        .build();
      Assert.fail("Exception expected as schema is invalid");
    } catch (Exception e) {
      //Expected
    }
  }

  @Test
  public void testFactoryInvalidDefaultInSchema() throws Exception {
    // schema has invalid default for field age.
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);

    try {
      new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.AVRO)
        .setCharset(Charset.forName("UTF-16"))
        .setConfig(SCHEMA_KEY, INVALID_SCHEMA_DEFAULTS)
        .build();
      Assert.fail("Exception expected as schema is invalid");
    } catch (Exception e) {
      //Expected
    }
  }

  @Test
  public void testSchemaInHeader() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
      true,
      baos,
      COMPRESSION_CODEC_DEFAULT,
      null,
      null,
      null,
      null,
      0
    );
    Record record = createRecord();
    record.getHeader().setAttribute(BaseAvroDataGenerator.AVRO_SCHEMA_HEADER, AVRO_SCHEMA);
    gen.write(record);
    gen.close();

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
  public void testConvertIntToStringInUnion() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
      true,
      baos,
      COMPRESSION_CODEC_DEFAULT,
      null,
      null,
      null,
      null,
      0
    );

    Map<String, Field> rootField = new HashMap<>();
    rootField.put("string", Field.create(Field.Type.INTEGER, 10));

    Record r = RecordCreator.create();
    r.getHeader().setAttribute(BaseAvroDataGenerator.AVRO_SCHEMA_HEADER, STRING_UNION_SCHEMA);
    r.set(Field.create(rootField));
    gen.write(r);
    gen.close();

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(null);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
      new SeekableByteArrayInput(baos.toByteArray()), reader);
    Assert.assertTrue(dataFileReader.hasNext());
    GenericRecord readRecord = dataFileReader.next();

    Assert.assertEquals(new Utf8("10"), readRecord.get("string"));
    Assert.assertFalse(dataFileReader.hasNext());
  }

  @Test(expected = DataGeneratorException.class)
  public void testSchemaInHeaderMissingHeader() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
      true,
      baos,
      COMPRESSION_CODEC_DEFAULT,
      null,
      null,
      null,
      null,
      0
    );
    Record record = createRecord();
    gen.write(record);
    gen.close();
  }

  @Test
  public void testSchemaInHeaderDifferentSchemaInHeader() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
      true,
      baos,
      COMPRESSION_CODEC_DEFAULT,
      null,
      null,
      null,
      null,
      0
    );
    Record record = createRecord();

    // Write first record (initialize all internal structures)
    record.getHeader().setAttribute(BaseAvroDataGenerator.AVRO_SCHEMA_HEADER, AVRO_SCHEMA);
    gen.write(record);

    try {
      // Second record with different schema should throw an exception
      record.getHeader().setAttribute(BaseAvroDataGenerator.AVRO_SCHEMA_HEADER, RECORD_SCHEMA);
      gen.write(record);
      Assert.fail("Expected exception to be thrown.");
    } catch(DataGeneratorException e) {
      Assert.assertTrue(e.getMessage().contains("AVRO_GENERATOR_04"));
    }

    gen.close();
  }

  @Test
  public void testAvroGeneratorDecimalType() throws Exception {
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("decimal", Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(1.5)));
    Record record = RecordCreator.create();
    record.set(Field.create(map));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
      false,
      baos,
      COMPRESSION_CODEC_DEFAULT,
      DECIMAL_SCHEMA,
      new HashMap<String, Object>(),
      null,
      null,
      0
    );
    gen.write(record);
    gen.close();

    //reader schema must be extracted from the data file
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(null);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
        new SeekableByteArrayInput(baos.toByteArray()), reader);
    Assert.assertTrue(dataFileReader.hasNext());
    GenericRecord readRecord = dataFileReader.next();

    Assert.assertArrayEquals(new byte[] {0x0F}, ((ByteBuffer)readRecord.get("decimal")).array());
    Assert.assertFalse(dataFileReader.hasNext());
  }

  @Test
  public void testAvroGeneratorDateType() throws Exception {
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("d", Field.create(Field.Type.DATE, new Date(116, 0, 1)));
    Record record = RecordCreator.create();
    record.set(Field.create(map));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
      false,
      baos,
      COMPRESSION_CODEC_DEFAULT,
      DATE_SCHEMA,
      new HashMap<String, Object>(),
      null,
      null,
      0
    );
    gen.write(record);
    gen.close();

    //reader schema must be extracted from the data file
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(null);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
        new SeekableByteArrayInput(baos.toByteArray()), reader);
    Assert.assertTrue(dataFileReader.hasNext());
    GenericRecord readRecord = dataFileReader.next();

    Assert.assertEquals(16801, readRecord.get("d"));
    Assert.assertFalse(dataFileReader.hasNext());
  }

  @Test
  public void testAvroGeneratorShortType() throws Exception {
    final String SCHEMA_JSON = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"WithDecimal\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"short\", \"type\": \"int\"}"
    +"]}";
    final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("short", Field.create(Field.Type.SHORT, (short)1));
    Record record = RecordCreator.create();
    record.set(Field.create(map));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
      false,
      baos,
      COMPRESSION_CODEC_DEFAULT,
      SCHEMA,
      new HashMap<String, Object>(),
      null,
      null,
      0
    );
    gen.write(record);
    gen.close();

    //reader schema must be extracted from the data file
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(null);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
        new SeekableByteArrayInput(baos.toByteArray()), reader);
    Assert.assertTrue(dataFileReader.hasNext());
    GenericRecord readRecord = dataFileReader.next();

    Object retrievedField = readRecord.get("short");
    Assert.assertEquals(1, retrievedField);

    Assert.assertFalse(dataFileReader.hasNext());
  }

  @Test
  public void testAvroGeneratorUnionWithShortType() throws Exception {
    final String SCHEMA_JSON = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"WithDecimal\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"short\", \"type\": [\"int\", \"null\"]}"
    +"]}";
    final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("short", Field.create(Field.Type.SHORT, (short)1));
    Record record = RecordCreator.create();
    record.set(Field.create(map));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataGenerator gen = new AvroDataOutputStreamGenerator(
      false,
      baos,
      COMPRESSION_CODEC_DEFAULT,
      SCHEMA,
      new HashMap<String, Object>(),
      null,
      null,
      0
    );

    try {
      gen.write(record);
      Assert.fail("Expected exception when writing SHORT into union");
    } catch(DataGeneratorException e) {
      Assert.assertNotNull(e);
      Assert.assertEquals(CommonError.CMN_0106, e.getErrorCode());
      Assert.assertEquals(e.getMessage(), "CMN_0106 - Error resolving union for SDC Type SHORT (java class java.lang.Short) against schema [\"int\",\"null\"]: org.apache.avro.AvroRuntimeException: Unknown datum type java.lang.Short: 1");
    } finally {
      gen.close();
    }
  }

}
