/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestAvroTypeUtil {

  @Test
  public void testCreateBooleanField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"boolean\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, true);
    Assert.assertEquals(Field.Type.BOOLEAN, field.getType());
    Assert.assertEquals(true, field.getValueAsBoolean());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "/", avroSchema);
    Assert.assertTrue(avroObject instanceof Boolean);
    Assert.assertEquals(true, avroObject);
  }

  @Test
  public void testCreateDoubleField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"double\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, 345823746923.863423);
    Assert.assertEquals(Field.Type.DOUBLE, field.getType());
    Assert.assertTrue(345823746923.863423 == field.getValueAsDouble());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "/", avroSchema);
    Assert.assertTrue(avroObject instanceof Double);
    Assert.assertTrue(345823746923.863423 == (Double) avroObject);
  }

  @Test
  public void testCreateIntegerField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"int\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, 34582);
    Assert.assertEquals(Field.Type.INTEGER, field.getType());
    Assert.assertTrue(34582 == field.getValueAsInteger());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "/", avroSchema);
    Assert.assertTrue(avroObject instanceof Integer);
    Assert.assertTrue(34582 == (Integer)avroObject);
  }

  @Test
  public void testCreateLongField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"long\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, 3458236L);
    Assert.assertEquals(Field.Type.LONG, field.getType());
    Assert.assertEquals(3458236L, field.getValueAsLong());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "/", avroSchema);
    Assert.assertTrue(avroObject instanceof Long);
    Assert.assertTrue(3458236L == (Long) avroObject);
  }

  @Test
  public void testCreateStringField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"string\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, "Hari");
    Assert.assertEquals(Field.Type.STRING, field.getType());
    Assert.assertEquals("Hari", field.getValueAsString());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "/", avroSchema);
    Assert.assertTrue(avroObject instanceof String);
    Assert.assertEquals("Hari", avroObject);
  }

  @Test
  public void testCreateEnumField() throws Exception {
    String schema = "{ \"type\": \"enum\",\n" +
      "  \"name\": \"Suit\",\n" +
      "  \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]\n" +
      "}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    GenericData.EnumSymbol enumSymbol = new GenericData.EnumSymbol(avroSchema, "CLUBS");
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, enumSymbol);

    Assert.assertEquals(Field.Type.STRING, field.getType());
    Assert.assertEquals("CLUBS", field.getValueAsString());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "/", avroSchema);
    Assert.assertTrue(avroObject instanceof GenericData.EnumSymbol);
    Assert.assertEquals("CLUBS", avroObject.toString());
  }

  @Test
  public void testCreateFixedField() throws Exception {
    byte[] bytes = new byte[16];
    for(int i = 0; i < 16; i++) {
      bytes[i] = (byte)i;
    }

    String schema = "{\"type\": \"fixed\", \"size\": 16, \"name\": \"md5\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    GenericData.Fixed fixed = new GenericData.Fixed(avroSchema, bytes);

    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, fixed);

    Assert.assertEquals(Field.Type.BYTE_ARRAY, field.getType());
    byte[] valueAsByteArray = field.getValueAsByteArray();
    Assert.assertEquals(16, valueAsByteArray.length);
    for(int i = 0; i < 16; i++) {
      Assert.assertEquals(i, valueAsByteArray[i]);
    }

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "/", avroSchema);
    Assert.assertTrue(avroObject instanceof GenericData.Fixed);
    GenericData.Fixed result = (GenericData.Fixed) avroObject;

    byte[] bytes1 = result.bytes();
    for(int i = 0; i < 16; i++) {
      Assert.assertEquals(i, bytes1[i]);
    }

  }

  @Test
  public void testCreateBytesField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"bytes\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, ByteBuffer.wrap("Hari".getBytes()));
    Assert.assertEquals(Field.Type.BYTE_ARRAY, field.getType());
    Assert.assertTrue(Arrays.equals("Hari".getBytes(), field.getValueAsByteArray()));

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "/", avroSchema);
    Assert.assertTrue(avroObject instanceof ByteBuffer);
    Assert.assertTrue(Arrays.equals("Hari".getBytes(), ((ByteBuffer) avroObject).array()));
  }

  @Test
  public void testCreateNullField() throws Exception {
    Schema schema = Schema.create(Schema.Type.NULL);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, schema, null);
    Assert.assertEquals(Field.Type.MAP, field.getType());
    Assert.assertEquals(null, field.getValue());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "/", schema);
    Assert.assertNull(avroObject);
  }

  @Test
  public void testCreateListPrimitiveField() throws Exception {
    String schema = "{\"type\": \"array\", \"items\": \"string\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, Arrays.asList("Hari", "Kiran"));

    Assert.assertEquals(Field.Type.LIST, field.getType());
    List<Field> valueAsList = field.getValueAsList();
    Assert.assertEquals(Field.Type.STRING, valueAsList.get(0).getType());
    Assert.assertEquals("Hari", valueAsList.get(0).getValueAsString());
    Assert.assertEquals(Field.Type.STRING, valueAsList.get(1).getType());
    Assert.assertEquals("Kiran", valueAsList.get(1).getValueAsString());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "", avroSchema);
    Assert.assertTrue(avroObject instanceof List<?>);

    List<String> listString = (List<String>) avroObject;
    Assert.assertEquals("Hari", listString.get(0));
    Assert.assertEquals("Kiran",listString.get(1));
  }

  @Test
  public void testCreateMapPrimitiveField() throws Exception {
    String schema = "{\"type\":\"map\",\"values\":\"int\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);

    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, ImmutableMap.of(new Utf8("Hari"), 1, new Utf8("Kiran"), 2));
    Assert.assertEquals(Field.Type.MAP, field.getType());
    Map<String, Field> valueAsMap = field.getValueAsMap();

    Assert.assertTrue(valueAsMap.containsKey("Hari"));
    Field hari = valueAsMap.get("Hari");
    Assert.assertEquals(Field.Type.INTEGER, hari.getType());
    Assert.assertEquals(1, hari.getValueAsInteger());

    Assert.assertTrue(valueAsMap.containsKey("Kiran"));
    hari = valueAsMap.get("Kiran");
    Assert.assertEquals(Field.Type.INTEGER, hari.getType());
    Assert.assertEquals(2, hari.getValueAsInteger());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "", avroSchema);
    Assert.assertTrue(avroObject instanceof Map<?,?>);

    Map<String, Integer> map = (Map<String, Integer>) avroObject;
    Assert.assertTrue(map.containsKey("Hari"));
    Assert.assertEquals(1, (int)map.get("Hari"));
    Assert.assertTrue(map.containsKey("Kiran"));
    Assert.assertEquals(2, (int)map.get("Kiran"));
  }

  @Test
  public void testCreateRecordField() throws StageException {
    String schema = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"name\", \"type\": \"string\"},\n"
      +" {\"name\": \"age\", \"type\": \"int\"},\n"
      +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
      +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
      +"]}";

    Schema avroSchema = new Schema.Parser().parse(schema);

    GenericRecord boss = new GenericData.Record(avroSchema);
    boss.put("name", "boss");
    boss.put("age", 60);
    boss.put("emails", ImmutableList.of("boss@ss.com", "boss2@ss.com"));

    GenericRecord genericRecord = new GenericData.Record(avroSchema);
    genericRecord.put("name", "hari");
    genericRecord.put("age", 20);
    genericRecord.put("emails", ImmutableList.of("hari@ss.com", "hari2@ss.com"));
    genericRecord.put("boss", boss);

    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, genericRecord);
    Assert.assertEquals(Field.Type.MAP, field.getType());
    Map<String, Field> map = field.getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("hari", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(20, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    List<Field> emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("hari@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("hari2@ss.com", emails.get(1).getValueAsString());

    Assert.assertTrue(map.containsKey("boss"));
    map = map.get("boss").getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("boss", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(60, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("boss@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("boss2@ss.com", emails.get(1).getValueAsString());

    //generic record has boss of union type /boss with type index 0
    //boss record has boss of union type /boss with type index 1 [null value]
    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/boss") != null);
    Assert.assertEquals("0", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/boss"));
    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/boss/boss") != null);
    Assert.assertEquals("1", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/boss/boss"));

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "", avroSchema);
    Assert.assertTrue(avroObject instanceof GenericRecord);

    GenericRecord result = (GenericRecord) avroObject;
    Assert.assertEquals("hari", result.get("name"));
    Assert.assertEquals(20, result.get("age"));
    List<String> resultEmails = (List<String>)result.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("hari@ss.com", resultEmails.get(0));
    Assert.assertEquals("hari2@ss.com", resultEmails.get(1));

    avroObject = result.get("boss");
    Assert.assertTrue(avroObject instanceof GenericRecord);

    result = (GenericRecord) avroObject;
    Assert.assertEquals("boss", result.get("name"));
    Assert.assertEquals(60, result.get("age"));
    resultEmails = (List<String>)result.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("boss@ss.com", resultEmails.get(0));
    Assert.assertEquals("boss2@ss.com", resultEmails.get(1));

  }

  @Test
  public void testCreateRecordListField() throws StageException {
    String schema = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"name\", \"type\": \"string\"},\n"
      +" {\"name\": \"age\", \"type\": \"int\"},\n"
      +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
      +" {\"name\": \"boss\", \"type\": [{\"type\": \"array\", \"items\": \"Employee\"},\"null\"]}\n"
      +"]}";

    Schema avroSchema = new Schema.Parser().parse(schema);

    GenericRecord boss1 = new GenericData.Record(avroSchema);
    boss1.put("name", "boss1");
    boss1.put("age", 60);
    boss1.put("emails", ImmutableList.of("boss@ss.com", "boss2@ss.com"));

    GenericRecord boss2 = new GenericData.Record(avroSchema);
    boss2.put("name", "boss2");
    boss2.put("age", 70);
    boss2.put("emails", ImmutableList.of("boss@ss.com", "boss2@ss.com"));

    GenericRecord genericRecord = new GenericData.Record(avroSchema);
    genericRecord.put("name", "hari");
    genericRecord.put("age", 20);
    genericRecord.put("emails", ImmutableList.of("hari@ss.com", "hari2@ss.com"));
    genericRecord.put("boss", ImmutableList.of(boss1, boss2));

    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, genericRecord);
    Assert.assertEquals(Field.Type.MAP, field.getType());
    Map<String, Field> map = field.getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("hari", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(20, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    List<Field> emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("hari@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("hari2@ss.com", emails.get(1).getValueAsString());

    Assert.assertTrue(map.containsKey("boss"));
    List<Field> bosses = map.get("boss").getValueAsList();
    Assert.assertEquals(2, bosses.size());

    map = bosses.get(0).getValueAsMap();

    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("boss1", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(60, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("boss@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("boss2@ss.com", emails.get(1).getValueAsString());

    map = bosses.get(1).getValueAsMap();

    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("boss2", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(70, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("boss@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("boss2@ss.com", emails.get(1).getValueAsString());

    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/boss") != null);
    Assert.assertEquals("0", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/boss"));
    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/boss[0]/boss") != null);
    Assert.assertEquals("1", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/boss[0]/boss"));
    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/boss[1]/boss") != null);
    Assert.assertEquals("1", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/boss[1]/boss"));

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "", avroSchema);
    Assert.assertTrue(avroObject instanceof GenericRecord);

    GenericRecord result = (GenericRecord) avroObject;
    Assert.assertEquals("hari", result.get("name"));
    Assert.assertEquals(20, result.get("age"));
    List<String> resultEmails = (List<String>)result.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("hari@ss.com", resultEmails.get(0));
    Assert.assertEquals("hari2@ss.com", resultEmails.get(1));

    avroObject = result.get("boss");
    Assert.assertTrue(avroObject instanceof List<?>);
    List<GenericRecord> list = (List<GenericRecord>) avroObject;
    Assert.assertEquals(2, list.size());

    result = list.get(0);

    Assert.assertEquals("boss1", result.get("name"));
    Assert.assertEquals(60, result.get("age"));
    resultEmails = (List<String>)result.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("boss@ss.com", resultEmails.get(0));
    Assert.assertEquals("boss2@ss.com", resultEmails.get(1));

    result = list.get(1);

    Assert.assertEquals("boss2", result.get("name"));
    Assert.assertEquals(70, result.get("age"));
    resultEmails = (List<String>)result.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("boss@ss.com", resultEmails.get(0));
    Assert.assertEquals("boss2@ss.com", resultEmails.get(1));
  }

  @Test
  public void testCreateListRecordField() throws StageException {

    String recordSchema = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"name\", \"type\": \"string\"},\n"
      +" {\"name\": \"age\", \"type\": \"int\"},\n"
      +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
      +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
      +"]}";

    String listSchema = "{\"type\": \"array\", \"items\": " +
      recordSchema + "}";

    Schema avroRecordSchema = new Schema.Parser().parse(recordSchema);
    Schema avroListSchema = new Schema.Parser().parse(listSchema);

    GenericRecord boss = new GenericData.Record(avroRecordSchema);
    boss.put("name", "boss");
    boss.put("age", 60);
    boss.put("emails", ImmutableList.of("boss@ss.com", "boss2@ss.com"));

    GenericRecord genericRecord1 = new GenericData.Record(avroRecordSchema);
    genericRecord1.put("name", "kiran");
    genericRecord1.put("age", 30);
    genericRecord1.put("emails", ImmutableList.of("kiran@ss.com", "kiran2@ss.com"));
    genericRecord1.put("boss", boss);

    GenericRecord genericRecord2 = new GenericData.Record(avroRecordSchema);
    genericRecord2.put("name", "hari");
    genericRecord2.put("age", 20);
    genericRecord2.put("emails", ImmutableList.of("hari@ss.com", "hari2@ss.com"));
    genericRecord2.put("boss", boss);

    Record record = RecordCreator.create();
    Field root = AvroTypeUtil.avroToSdcField(record, avroListSchema,
      ImmutableList.of(genericRecord2, genericRecord1));
    Assert.assertEquals(Field.Type.LIST, root.getType());
    List<Field> valueAsList = root.getValueAsList();
    Assert.assertEquals(2, valueAsList.size());

    Field field = valueAsList.get(1);
    Assert.assertEquals(Field.Type.MAP, field.getType());
    Map<String, Field> map = field.getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("kiran", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(30, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    List<Field> emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("kiran@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("kiran2@ss.com", emails.get(1).getValueAsString());

    Assert.assertTrue(map.containsKey("boss"));
    map = map.get("boss").getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("boss", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(60, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("boss@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("boss2@ss.com", emails.get(1).getValueAsString());


    field = valueAsList.get(0);
    Assert.assertEquals(Field.Type.MAP, field.getType());
    map = field.getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("hari", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(20, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("hari@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("hari2@ss.com", emails.get(1).getValueAsString());

    Assert.assertTrue(map.containsKey("boss"));
    map = map.get("boss").getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("boss", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(60, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("boss@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("boss2@ss.com", emails.get(1).getValueAsString());

    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "[0]/boss") != null);
    Assert.assertEquals("0", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "[0]/boss"));
    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "[0]/boss/boss") != null);
    Assert.assertEquals("1", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "[0]/boss/boss"));

    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "[1]/boss") != null);
    Assert.assertEquals("0", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "[1]/boss"));
    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "[1]/boss/boss") != null);
    Assert.assertEquals("1", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "[1]/boss/boss"));

    record.set(root);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "", avroListSchema);
    Assert.assertTrue(avroObject instanceof List<?>);

    List<GenericRecord> genericRecordList = (List<GenericRecord>) avroObject;
    Assert.assertEquals(2, genericRecordList.size());

    GenericRecord result1 = genericRecordList.get(0);
    Assert.assertEquals("hari", result1.get("name"));
    Assert.assertEquals(20, result1.get("age"));
    List<String> resultEmails = (List<String>)result1.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("hari@ss.com", resultEmails.get(0));
    Assert.assertEquals("hari2@ss.com", resultEmails.get(1));

    avroObject = result1.get("boss");
    Assert.assertTrue(avroObject instanceof GenericRecord);

    result1 = (GenericRecord) avroObject;
    Assert.assertEquals("boss", result1.get("name"));
    Assert.assertEquals(60, result1.get("age"));
    resultEmails = (List<String>)result1.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("boss@ss.com", resultEmails.get(0));
    Assert.assertEquals("boss2@ss.com", resultEmails.get(1));

    GenericRecord result2 = genericRecordList.get(1);
    Assert.assertEquals("kiran", result2.get("name"));
    Assert.assertEquals(30, result2.get("age"));
    resultEmails = (List<String>)result2.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("kiran@ss.com", resultEmails.get(0));
    Assert.assertEquals("kiran2@ss.com", resultEmails.get(1));

    avroObject = result2.get("boss");
    Assert.assertTrue(avroObject instanceof GenericRecord);

    result2 = (GenericRecord) avroObject;
    Assert.assertEquals("boss", result2.get("name"));
    Assert.assertEquals(60, result2.get("age"));
    resultEmails = (List<String>)result2.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("boss@ss.com", resultEmails.get(0));
    Assert.assertEquals("boss2@ss.com", resultEmails.get(1));

  }

  @Test
  public void testCreateMapRecordField() throws StageException {

    String recordSchema = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"name\", \"type\": \"string\"},\n"
      +" {\"name\": \"age\", \"type\": \"int\"},\n"
      +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
      +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
      +"]}";

    String mapSchema = "{\"type\":\"map\",\"values\":\n" +
      recordSchema + "}";

    Schema avroRecordSchema = new Schema.Parser().parse(recordSchema);
    Schema avroMapSchema = new Schema.Parser().parse(mapSchema);

    GenericRecord boss = new GenericData.Record(avroRecordSchema);
    boss.put("name", "boss");
    boss.put("age", 60);
    boss.put("emails", ImmutableList.of("boss@ss.com", "boss2@ss.com"));

    GenericRecord genericRecord1 = new GenericData.Record(avroRecordSchema);
    genericRecord1.put("name", "kiran");
    genericRecord1.put("age", 30);
    genericRecord1.put("emails", ImmutableList.of("kiran@ss.com", "kiran2@ss.com"));
    genericRecord1.put("boss", boss);

    GenericRecord genericRecord2 = new GenericData.Record(avroRecordSchema);
    genericRecord2.put("name", "hari");
    genericRecord2.put("age", 20);
    genericRecord2.put("emails", ImmutableList.of("hari@ss.com", "hari2@ss.com"));
    genericRecord2.put("boss", boss);

    Record record = RecordCreator.create();
    Field root = AvroTypeUtil.avroToSdcField(record, avroMapSchema, ImmutableMap.of(new Utf8("Hari"), genericRecord2,
      new Utf8("Kiran"), genericRecord1));
    Assert.assertEquals(Field.Type.MAP, root.getType());

    Map<String, Field> valueAsMap = root.getValueAsMap();

    Assert.assertTrue(valueAsMap.containsKey("Hari"));
    Field field = valueAsMap.get("Hari");

    Assert.assertEquals(Field.Type.MAP, field.getType());
    Map<String, Field> map = field.getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("hari", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(20, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    List<Field> emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("hari@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("hari2@ss.com", emails.get(1).getValueAsString());

    Assert.assertTrue(map.containsKey("boss"));
    map = map.get("boss").getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("boss", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(60, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("boss@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("boss2@ss.com", emails.get(1).getValueAsString());

    Assert.assertTrue(valueAsMap.containsKey("Kiran"));
    field = valueAsMap.get("Kiran");

    Assert.assertEquals(Field.Type.MAP, field.getType());
    map = field.getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("kiran", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(30, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("kiran@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("kiran2@ss.com", emails.get(1).getValueAsString());

    Assert.assertTrue(map.containsKey("boss"));
    map = map.get("boss").getValueAsMap();
    Assert.assertTrue(map.containsKey("name"));
    Assert.assertEquals("boss", map.get("name").getValueAsString());
    Assert.assertTrue(map.containsKey("age"));
    Assert.assertEquals(60, map.get("age").getValueAsInteger());
    Assert.assertTrue(map.containsKey("emails"));
    emails = map.get("emails").getValueAsList();
    Assert.assertEquals(2, emails.size());
    Assert.assertTrue(emails.get(0).getType() == Field.Type.STRING);
    Assert.assertEquals("boss@ss.com", emails.get(0).getValueAsString());
    Assert.assertTrue(emails.get(1).getType() == Field.Type.STRING);
    Assert.assertEquals("boss2@ss.com", emails.get(1).getValueAsString());

    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/Hari/boss") != null);
    Assert.assertEquals("0", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/Hari/boss"));
    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/Hari/boss/boss") != null);
    Assert.assertEquals("1", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/Hari/boss/boss"));

    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/Kiran/boss") != null);
    Assert.assertEquals("0", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/Kiran/boss"));
    Assert.assertTrue(record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/Kiran/boss/boss") != null);
    Assert.assertEquals("1", record.getHeader().getAttribute(AvroTypeUtil.AVRO_UNION_TYPE_INDEX_PREFIX + "/Kiran/boss/boss"));

    record.set(root);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, "", avroMapSchema);

    Assert.assertTrue(avroObject instanceof Map);

    Map<String, GenericRecord> genericRecordMap = (Map<String, GenericRecord>) avroObject;

    Assert.assertTrue(genericRecordMap.containsKey("Hari"));
    GenericRecord result1 = genericRecordMap.get("Hari");

    Assert.assertEquals("hari", result1.get("name"));
    Assert.assertEquals(20, result1.get("age"));
    List<String> resultEmails = (List<String>)result1.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("hari@ss.com", resultEmails.get(0));
    Assert.assertEquals("hari2@ss.com", resultEmails.get(1));

    avroObject = result1.get("boss");
    Assert.assertTrue(avroObject instanceof GenericRecord);

    result1 = (GenericRecord) avroObject;
    Assert.assertEquals("boss", result1.get("name"));
    Assert.assertEquals(60, result1.get("age"));
    resultEmails = (List<String>)result1.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("boss@ss.com", resultEmails.get(0));
    Assert.assertEquals("boss2@ss.com", resultEmails.get(1));

    Assert.assertTrue(genericRecordMap.containsKey("Kiran"));
    GenericRecord result2 = genericRecordMap.get("Kiran");
    Assert.assertEquals("kiran", result2.get("name"));
    Assert.assertEquals(30, result2.get("age"));
    resultEmails = (List<String>)result2.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("kiran@ss.com", resultEmails.get(0));
    Assert.assertEquals("kiran2@ss.com", resultEmails.get(1));

    avroObject = result2.get("boss");
    Assert.assertTrue(avroObject instanceof GenericRecord);

    result2 = (GenericRecord) avroObject;
    Assert.assertEquals("boss", result2.get("name"));
    Assert.assertEquals(60, result2.get("age"));
    resultEmails = (List<String>)result2.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("boss@ss.com", resultEmails.get(0));
    Assert.assertEquals("boss2@ss.com", resultEmails.get(1));
  }

}
