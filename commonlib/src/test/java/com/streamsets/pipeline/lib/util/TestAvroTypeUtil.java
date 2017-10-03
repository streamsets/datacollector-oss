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
package com.streamsets.pipeline.lib.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.avro.Errors;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
    Assert.assertTrue(avroObject instanceof Integer);
    Assert.assertTrue(34582 == (Integer) avroObject);
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
    Assert.assertTrue(avroObject instanceof GenericData.EnumSymbol);
    Assert.assertEquals("CLUBS", avroObject.toString());
  }

  @Test
  public void testCreateFixedField() throws Exception {
    byte[] bytes = new byte[16];
    for (int i = 0; i < 16; i++) {
      bytes[i] = (byte) i;
    }

    String schema = "{\"type\": \"fixed\", \"size\": 16, \"name\": \"md5\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    GenericData.Fixed fixed = new GenericData.Fixed(avroSchema, bytes);

    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, fixed);

    Assert.assertEquals(Field.Type.BYTE_ARRAY, field.getType());
    byte[] valueAsByteArray = field.getValueAsByteArray();
    Assert.assertEquals(16, valueAsByteArray.length);
    for (int i = 0; i < 16; i++) {
      Assert.assertEquals(i, valueAsByteArray[i]);
    }

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
    Assert.assertTrue(avroObject instanceof GenericData.Fixed);
    GenericData.Fixed result = (GenericData.Fixed) avroObject;

    byte[] bytes1 = result.bytes();
    for (int i = 0; i < 16; i++) {
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, schema, new HashMap<String, Object>());
    Assert.assertNull(avroObject);
  }

  @Test
  public void testCreateDecimalField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 2, \"scale\": 1}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, BigDecimal.valueOf(1.5));
    Assert.assertEquals(Field.Type.DECIMAL, field.getType());
    Assert.assertEquals(BigDecimal.valueOf(1.5), field.getValueAsDecimal());
    Assert.assertEquals("decimal", field.getAttribute(AvroTypeUtil.FIELD_ATTRIBUTE_TYPE));

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
    Assert.assertTrue(avroObject instanceof ByteBuffer);
    Assert.assertArrayEquals(new byte[] {0x0F}, ((ByteBuffer)avroObject).array());
  }

  @Test
  public void testToAndFromAvroDecimal() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 2, \"scale\": 1}";

    BigDecimal expectedValue = BigDecimal.valueOf(1.5);

    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.DECIMAL, expectedValue));
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, Collections.emptyMap());

    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, avroObject);
    Assert.assertEquals(Field.Type.DECIMAL, field.getType());
    Assert.assertEquals(expectedValue, field.getValueAsDecimal());
    Assert.assertEquals("2", field.getAttribute(AvroTypeUtil.LOGICAL_TYPE_ATTR_PRECISION));
    Assert.assertEquals("1", field.getAttribute(AvroTypeUtil.LOGICAL_TYPE_ATTR_SCALE));
  }

  @Test
  public void testCreateDateField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"int\", \"logicalType\": \"date\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, new Date(116, 0, 1));
    Assert.assertEquals(Field.Type.DATE, field.getType());
    Assert.assertEquals("date", field.getAttribute(AvroTypeUtil.FIELD_ATTRIBUTE_TYPE));
    Assert.assertEquals(new Date(116, 0, 1), field.getValueAsDate());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
    Assert.assertTrue(avroObject instanceof Integer);
    Assert.assertEquals(16801, (int)avroObject);
  }

  @Test
  public void testCreateUnionWithNull() throws Exception {
    String schema = "[\"null\", \"string\"]";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, null);
    Assert.assertEquals(Field.Type.STRING, field.getType());
    Assert.assertEquals(null, field.getValue());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, Collections.emptyMap());
    Assert.assertNull(avroObject);
  }

  @Test
  public void testCreateUnionWithNullAndLogicalType() throws Exception {
    String schema = "[\"null\", {\"name\": \"name\", \"type\": \"int\", \"logicalType\": \"date\"}]";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, null);
    Assert.assertEquals(Field.Type.DATE, field.getType());
    Assert.assertEquals(null, field.getValue());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, Collections.emptyMap());
    Assert.assertNull(avroObject);
  }

  @Test
  public void testToAndFromAvroDate() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"int\", \"logicalType\": \"date\"}";

    Date expectedDate = new Date(116, 0, 1);

    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.DATE, expectedDate));

    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, Collections.emptyMap());
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, avroObject);
    Assert.assertEquals(Field.Type.DATE, field.getType());
    Assert.assertEquals(expectedDate, field.getValueAsDate());
  }

  @Test
  public void testCreateTimeMillisField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"int\", \"logicalType\": \"time-millis\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, 1000);
    Assert.assertEquals(Field.Type.TIME, field.getType());
    Assert.assertEquals("time-millis", field.getAttribute(AvroTypeUtil.FIELD_ATTRIBUTE_TYPE));
    Assert.assertEquals(new Date(1000L), field.getValueAsDate());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<>());
    Assert.assertTrue(avroObject instanceof Integer);
    Assert.assertEquals(1000, (int)avroObject);
  }

  @Test
  public void testCreateTimeMicrosField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"long\", \"logicalType\": \"time-micros\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, 1000);
    Assert.assertEquals(Field.Type.LONG, field.getType());
    Assert.assertEquals("time-micros", field.getAttribute(AvroTypeUtil.FIELD_ATTRIBUTE_TYPE));
    Assert.assertEquals(1000L, field.getValueAsLong());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<>());
    Assert.assertTrue(avroObject instanceof Long);
    Assert.assertEquals(1000, (long)avroObject);
  }

  @Test
  public void testCreateTimestampMillisField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\"}";
    Date date = new Date(116, 0, 1, 3, 30, 5);
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, date);
    Assert.assertEquals(Field.Type.DATETIME, field.getType());
    Assert.assertEquals("timestamp-millis", field.getAttribute(AvroTypeUtil.FIELD_ATTRIBUTE_TYPE));
    Assert.assertEquals(date, field.getValueAsDate());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<>());
    Assert.assertTrue(avroObject instanceof Long);
    Assert.assertEquals(date.getTime(), (long)avroObject);
  }

  @Test
  public void testCreateTimestampMicrosField() throws Exception {
    String schema = "{\"name\": \"name\", \"type\": \"long\", \"logicalType\": \"timestamp-micros\"}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Field field = AvroTypeUtil.avroToSdcField(record, avroSchema, 1000L);
    Assert.assertEquals(Field.Type.LONG, field.getType());
    Assert.assertEquals("timestamp-micros", field.getAttribute(AvroTypeUtil.FIELD_ATTRIBUTE_TYPE));
    Assert.assertEquals(1000L, field.getValueAsLong());

    record.set(field);
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<>());
    Assert.assertTrue(avroObject instanceof Long);
    Assert.assertEquals(1000L, (long)avroObject);
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
    Assert.assertTrue(avroObject instanceof List<?>);

    List<String> listString = (List<String>) avroObject;
    Assert.assertEquals("Hari", listString.get(0));
    Assert.assertEquals("Kiran", listString.get(1));
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
    Assert.assertTrue(avroObject instanceof Map<?, ?>);

    Map<String, Integer> map = (Map<String, Integer>) avroObject;
    Assert.assertTrue(map.containsKey("Hari"));
    Assert.assertEquals(1, (int) map.get("Hari"));
    Assert.assertTrue(map.containsKey("Kiran"));
    Assert.assertEquals(2, (int) map.get("Kiran"));
  }

  @Test
  public void testCreateRecordField() throws StageException, IOException {
    String schema = "{\n"
      + "\"type\": \"record\",\n"
      + "\"name\": \"Employee\",\n"
      + "\"fields\": [\n"
      + " {\"name\": \"name\", \"type\": \"string\"},\n"
      + " {\"name\": \"age\", \"type\": \"int\"},\n"
      + " {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
      + " {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
      + "]}";

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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
    Assert.assertTrue(avroObject instanceof GenericRecord);

    GenericRecord result = (GenericRecord) avroObject;
    Assert.assertEquals("hari", result.get("name"));
    Assert.assertEquals(20, result.get("age"));
    List<String> resultEmails = (List<String>) result.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("hari@ss.com", resultEmails.get(0));
    Assert.assertEquals("hari2@ss.com", resultEmails.get(1));

    avroObject = result.get("boss");
    Assert.assertTrue(avroObject instanceof GenericRecord);

    result = (GenericRecord) avroObject;
    Assert.assertEquals("boss", result.get("name"));
    Assert.assertEquals(60, result.get("age"));
    resultEmails = (List<String>) result.get("emails");
    Assert.assertEquals(2, resultEmails.size());

    Assert.assertEquals("boss@ss.com", resultEmails.get(0));
    Assert.assertEquals("boss2@ss.com", resultEmails.get(1));

  }

  @Test
  public void testCreateRecordListField() throws StageException, IOException {
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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
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
  public void testCreateListRecordField() throws StageException, IOException {

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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroListSchema, new HashMap<String, Object>());
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
  public void testCreateMapRecordField() throws StageException, IOException {

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
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, avroMapSchema, new HashMap<String, Object>());

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

  @Test
  public void testBestEffortResolve1() throws StageException, IOException {

    String schemaString = "{\n" +
      "  \"type\" : \"record\",\n" +
      "  \"name\" : \"TopLevel\",\n" +
      "  \"namespace\" : \"com.streamsets.demo\",\n" +
      "  \"fields\" : [ {\n" +
      "    \"name\" : \"foo\",\n" +
      "    \"type\" : [ {\n" +
      "      \"type\" : \"record\",\n" +
      "      \"name\" : \"Foo\",\n" +
      "      \"fields\" : [ {\n" +
      "        \"name\" : \"bar\",\n" +
      "        \"type\" : \"int\"\n" +
      "      } ]\n" +
      "    }, \"null\" ]\n" +
      "  } ]\n" +
      "}";

    // create sdc record matching the above schema
    Map<String, Field> barField = new HashMap<>();
    barField.put("bar", Field.create(45237));
    Map<String, Field> fooField = new HashMap<>();
    fooField.put("foo", Field.create(barField));
    Record record = RecordCreator.create();
    record.set(Field.create(fooField));

    Schema schema = new Schema.Parser().parse(schemaString);

    //convert record to avro record
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, schema, new HashMap<String, Object>());

    GenericRecord avroRecord = (GenericRecord) avroObject;
    GenericRecord foo = (GenericRecord) avroRecord.get("foo");
    Object bar = foo.get("bar");
    Assert.assertEquals(45237, bar);
  }

  @Test
  public void testBestEffortResolve2() throws StageException, IOException {

    String schemaString = "{\n" +
      "  \"type\" : \"record\",\n" +
      "  \"name\" : \"TopLevel\",\n" +
      "  \"namespace\" : \"com.streamsets.demo\",\n" +
      "  \"fields\" : [ {\n" +
      "    \"name\" : \"foo\",\n" +
      "    \"type\" : [ {\n" +
      "      \"type\" : \"record\",\n" +
      "      \"name\" : \"Foo\",\n" +
      "      \"fields\" : [ {\n" +
      "        \"name\" : \"bar\",\n" +
      "        \"type\" : \"int\"\n" +
      "      } ]\n" +
      "    }, {\"type\" : \"map\", \"values\" : \"int\"} ]\n" +
      "  } ]\n" +
      "}";

    // create sdc record matching the above schema
    Map<String, Field> barField = new HashMap<>();
    barField.put("bar", Field.create(45237));
    Map<String, Field> fooField = new HashMap<>();
    fooField.put("foo", Field.create(barField));
    Record record = RecordCreator.create();
    record.set(Field.create(fooField));

    Schema schema = new Schema.Parser().parse(schemaString);

    // convert record to avro record
    Object avroObject = AvroTypeUtil.sdcRecordToAvro(record, schema, new HashMap<String, Object>());

    GenericRecord avroRecord = (GenericRecord) avroObject;

    // Avro will match this to map schema type from the union before going to the best effort resolve by sdc
    Map<String, Integer> foo = (Map<String, Integer>) avroRecord.get("foo");
    Object bar = foo.get("bar");
    Assert.assertEquals(45237, bar);
  }

  @Test
  public void testGetDefaultValuesFromSchema() throws IOException, StageException {

    // Tests the AvroTypeUtil.getDefaultValuesFromSchema method

    String schema = "{\n" +
      "  \"namespace\": \"my.namespace\",\n" +
      "  \"name\": \"Employee\",\n" +
      "  \"type\" :  \"record\",\n" +
      "  \"fields\" : [\n" +
      "     {\"name\": \"id\", \"type\": \"int\", \"default\": 345678},\n" +
      "     {\"name\": \"name\", \"type\": \"string\", \"default\": \"e1\"},\n" +
      "     {\"name\": \"addresses\", \"type\": {\n" +
      "        \"type\": \"array\",\n" +
      "        \"items\": {\n" +
      "            \"type\": \"record\",\n" +
      "            \"namespace\": \"not.my.namespace\",\n" +
      "            \"name\": \"Address\",\n" +
      "            \"fields\": [\n" +
      "                {\"name\": \"street\", \"type\": \"string\", \"default\": \"2 bryant\"},\n" +
      "                {\"name\": \"phones\", \"type\": {\n" +
      "                    \"type\": \"array\",\n" +
      "                    \"items\": {\n" +
      "                        \"type\": \"record\",\n" +
      "                        \"name\": \"Phone\",\n" +
      "                        \"fields\": [\n" +
      "                            {\"name\": \"home\", \"type\": \"string\", \"default\": \"8675309\"},\n" +
      "                            {\"name\": \"mobile\", \"type\": \"string\", \"default\": \"8675308\"}\n" +
      "                        ]\n" +
      "                    }" +
      "                }}\n" +
      "            ]\n" +
      "        }\n" +
      "     }}\n" +
      "  ]\n" +
      "}";

    Schema avroSchema = new Schema.Parser().parse(schema);
    Map<String, Object> defaultValuesFromSchema = AvroTypeUtil.getDefaultValuesFromSchema(avroSchema, new HashSet<String>());

    Assert.assertEquals(5, defaultValuesFromSchema.size());
    Assert.assertEquals(345678, defaultValuesFromSchema.get("my.namespace.Employee.id"));
    Assert.assertEquals("e1", defaultValuesFromSchema.get("my.namespace.Employee.name"));
    Assert.assertEquals("2 bryant", defaultValuesFromSchema.get("not.my.namespace.Address.street"));
    Assert.assertEquals("8675309", defaultValuesFromSchema.get("not.my.namespace.Phone.home"));
    // Phone inherits namespace from the enclosing type
    Assert.assertEquals("8675308", defaultValuesFromSchema.get("not.my.namespace.Phone.mobile"));
  }

  @Test
  public void testDefaultsForPrimitiveSchema() throws IOException {

    // default values are read from a record schema for its fields. Since this schema is not a record, no default
    // values are extracted form this schema.
    String schema = "{\"name\": \"name\", \"type\": \"boolean\", \"default\" : true}";
    Schema parse = new Schema.Parser().parse(schema);
    Map<String, Object> defaultValuesFromSchema = AvroTypeUtil.getDefaultValuesFromSchema(parse, new HashSet<String>());

    Assert.assertTrue(defaultValuesFromSchema.isEmpty());

  }

  @Test
  public void testDefaultsForArraySchema() throws IOException {

    // default values are read from a record schema for its fields. Since this schema is not a record, no default
    // values are extracted form this schema.
    String schema = "{ \"type\": \"array\", \"items\": {" +
        "  \"type\": \"record\"," +
        "  \"name\": \"Phone\"," +
        "  \"fields\": [" +
        "     {\"name\": \"home\", \"type\": \"string\", \"default\": \"8675309\"}," +
        "     {\"name\": \"mobile\", \"type\": \"string\", \"default\": \"8675308\"}" +
        "    ]" +
        "  }" +
        "}";
    Schema parse = new Schema.Parser().parse(schema);
    Map<String, Object> defaultValuesFromSchema = AvroTypeUtil.getDefaultValuesFromSchema(parse, new HashSet<String>());

    Assert.assertEquals(2, defaultValuesFromSchema.size());
    Assert.assertEquals("8675309", defaultValuesFromSchema.get("Phone.home"));
    Assert.assertEquals("8675308", defaultValuesFromSchema.get("Phone.mobile"));

  }

  @Test
  public void testDefaultsForMapSchema() throws IOException {

    // default values are read from a record schema for its fields. Since this schema is not a record, no default
    // values are extracted form this schema.
    String schema = "{ \"type\": \"map\", \"values\": {" +
      "  \"type\": \"record\"," +
      "  \"name\": \"Phone\"," +
      "  \"fields\": [" +
      "     {\"name\": \"home\", \"type\": \"string\", \"default\": \"8675309\"}," +
      "     {\"name\": \"mobile\", \"type\": \"string\", \"default\": \"8675308\"}" +
      "    ]" +
      "  }" +
      "}";
    Schema parse = new Schema.Parser().parse(schema);
    Map<String, Object> defaultValuesFromSchema = AvroTypeUtil.getDefaultValuesFromSchema(parse, new HashSet<String>());

    Assert.assertEquals(2, defaultValuesFromSchema.size());
    Assert.assertEquals("8675309", defaultValuesFromSchema.get("Phone.home"));
    Assert.assertEquals("8675308", defaultValuesFromSchema.get("Phone.mobile"));

  }

  @Test
  public void testDefaultsForRecordSchema() throws StageException, IOException {

    // tests schema with default values for most types

    String schema = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"name\", \"type\": \"string\", \"default\": \"Hello\"},\n"
      +" {\"name\": \"age\", \"type\": \"int\", \"default\": 25},\n"
      +" {\"name\": \"resident\", \"type\": \"boolean\", \"default\": false},\n"
      +" {\"name\": \"enum\",\"type\":{\"type\":\"enum\",\"name\":\"Suit\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]}, \"default\": \"DIAMONDS\"},\n"
      +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}, \"default\" : [\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]},\n"
      +" {\"name\": \"phones\", \"type\": {\"type\": \"map\", \"values\": \"long\"}, \"default\" : {\"home\" : 8675309, \"mobile\" : 8675308}},\n"
      +" {\"name\": \"boss\", \"type\": [\"null\", \"Employee\"], \"default\" : null}\n"
      +"]}";

    Schema avroSchema = new Schema.Parser().parse(schema);

    Record record = RecordCreator.create();
    Map<String, Field> employee = new HashMap<>();
    // create empty record with no fields
    record.set(Field.create(employee));

    Object avroObject = AvroTypeUtil.sdcRecordToAvro(
      record,
      avroSchema,
      AvroTypeUtil.getDefaultValuesFromSchema(avroSchema, new HashSet<String>())
    );

    Assert.assertTrue(avroObject instanceof GenericRecord);

    // expected fields with default values
    GenericRecord result = (GenericRecord) avroObject;
    Assert.assertEquals("Hello", result.get("name"));
    Assert.assertEquals(25, result.get("age"));
    Assert.assertEquals(false, result.get("resident"));
    Assert.assertEquals("DIAMONDS", result.get("enum"));

    List<String> emails = (List<String>) result.get("emails");
    Assert.assertEquals(4, emails.size());
    Assert.assertEquals("SPADES", emails.get(0));
    Assert.assertEquals("HEARTS", emails.get(1));
    Assert.assertEquals("DIAMONDS", emails.get(2));
    Assert.assertEquals("CLUBS", emails.get(3));

    Assert.assertEquals(null, result.get("boss"));

    Map<String, Object> phones = (Map<String, Object>) result.get("phones");
    Assert.assertEquals(8675309, (long)phones.get("home"));
    Assert.assertEquals(8675308, (long)phones.get("mobile"));
  }

  @Test
  public void testDefaultsForUnionSchema() throws IOException {

    // default values are read from a record schema for its fields. Since this schema is not a record, no default
    // values are extracted form this schema.
    String schema = "[\"null\", {\"type\": \"record\"," +
      "  \"name\": \"Phone\"," +
      "  \"fields\": [" +
      "     {\"name\": \"home\", \"type\": \"string\", \"default\": \"8675309\"}," +
      "     {\"name\": \"mobile\", \"type\": \"string\", \"default\": \"8675308\"}" +
      "    ]" +
      "  }]";
    Schema parse = new Schema.Parser().parse(schema);
    Map<String, Object> defaultValuesFromSchema = AvroTypeUtil.getDefaultValuesFromSchema(parse, new HashSet<String>());

    Assert.assertEquals(2, defaultValuesFromSchema.size());
    Assert.assertEquals("8675309", defaultValuesFromSchema.get("Phone.home"));
    Assert.assertEquals("8675308", defaultValuesFromSchema.get("Phone.mobile"));

  }

  @Test
  public void testSdcToAvroWithDefaultValues() throws IOException, StageException {

    // tests the AvroTypeUtil.sdcRecordToAvro method which takes in the default value map

    String schema = "{\n" +
      "  \"namespace\": \"my.namespace\",\n" +
      "  \"name\": \"Employee\",\n" +
      "  \"type\" :  \"record\",\n" +
      "  \"fields\" : [\n" +
      "     {\"name\": \"id\", \"type\": \"int\", \"default\": 345678},\n" +
      "     {\"name\": \"name\", \"type\": \"string\", \"default\": \"e1\"},\n" +
      "     {\"name\": \"addresses\", \"type\": {\n" +
      "        \"type\": \"array\",\n" +
      "        \"items\": {\n" +
      "            \"type\": \"record\",\n" +
      "            \"namespace\": \"my.namespace\",\n" +
      "            \"name\": \"Address\",\n" +
      "            \"fields\": [\n" +
      "                {\"name\": \"street\", \"type\": \"string\", \"default\": \"2 bryant\"},\n" +
      "                {\"name\": \"phones\", \"type\": {\n" +
      "                    \"type\": \"array\",\n" +
      "                    \"items\": {\n" +
      "                        \"type\": \"record\",\n" +
      "                        \"name\": \"Phone\",\n" +
      "                        \"fields\": [\n" +
      "                            {\"name\": \"home\", \"type\": \"string\", \"default\": \"8675309\"},\n" +
      "                            {\"name\": \"mobile\", \"type\": \"string\", \"default\": \"8675308\"}\n" +
      "                        ]\n" +
      "                    }" +
      "                }}\n" +
      "            ]\n" +
      "        }\n" +
      "     }}\n" +
      "  ]\n" +
      "}";

    Schema avroSchema = new Schema.Parser().parse(schema);

    Record record = RecordCreator.create();
    Map<String, Field> employee = new HashMap<>();
    Map<String, Field> address = new HashMap<>();
    Map<String, Field> phone = new HashMap<>();
    List<Field> phones = new ArrayList<>();
    phones.add(Field.create(phone));
    address.put("phones", Field.create(phones));
    List<Field> addresses = new ArrayList<>();
    addresses.add(Field.create(address));
    employee.put("addresses", Field.create(addresses));
    addresses.add(Field.create(addresses));
    record.set(Field.create(employee));

    Object avroObject = AvroTypeUtil.sdcRecordToAvro(
      record,
      avroSchema,
      AvroTypeUtil.getDefaultValuesFromSchema(avroSchema, new HashSet<String>())
    );

    Assert.assertTrue(avroObject instanceof GenericRecord);
    GenericRecord result = (GenericRecord) avroObject;
    Assert.assertEquals("e1", result.get("name"));
    Assert.assertEquals(345678, result.get("id"));
    List<GenericRecord> adds = (List<GenericRecord>) result.get("addresses");
    Assert.assertEquals(1, adds.size());

    result = adds.get(0);
    Assert.assertEquals("2 bryant", result.get("street"));

    List<GenericRecord> phs = (List<GenericRecord>) result.get("phones");
    Assert.assertEquals(1, phs.size());
    result = phs.get(0);
    Assert.assertEquals("8675309", result.get("home"));
    Assert.assertEquals("8675308", result.get("mobile"));
  }

  @Test
  public void testInvalidUnionDefault1() {

    // tests that union type with invalid default values are flagged
    // in this case the default value can only be null since that is the first type in the union
    String schema = "{\n"
      + "\"type\": \"record\",\n"
      + "\"name\": \"Employee\",\n"
      + "\"fields\": [\n"
      + " {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": \"Hello\"}\n"
      + "]}";

    Schema avroSchema = new Schema.Parser().parse(schema);
    try {
      AvroTypeUtil.getDefaultValuesFromSchema(avroSchema, new HashSet<String>());
      Assert.fail("IOException expected as the Avro Schema in invalid. " +
        "Default value must be of the first type in the union");
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testInvalidUnionDefault2() {

    // tests that union type with invalid default values are flagged
    // in this case the default value can only be null since that is the first type in the union
    String schema = "{\n"
      + "\"type\": \"record\",\n"
      + "\"name\": \"Employee\",\n"
      + "\"fields\": [\n"
      + " {\"name\": \"name\", \"type\": [\"string\", \"null\"], \"default\": null}\n"
      + "]}";

    Schema avroSchema = new Schema.Parser().parse(schema);
    try {
      AvroTypeUtil.getDefaultValuesFromSchema(avroSchema, new HashSet<String>());
    } catch (IOException e) {
      Assert.fail("Exception is not expected as null is a valid default for string type");
    }
  }

  @Test
  public void testNoDefaultsNoFields() throws StageException, IOException {

    // tests that when there are no defaults in schema and record is missing those fields then it results in an
    // exception

    String schema = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"name\", \"type\": \"string\"}\n"
      +"]}";

    checkForMissingFieldsException(schema);

    schema = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"boss\", \"type\": \"Employee\"}\n"
      +"]}";

    checkForMissingFieldsException(schema);

    schema = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n"
      +"]}";

    checkForMissingFieldsException(schema);

    schema = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"phones\", \"type\": {\"type\": \"map\", \"values\": \"long\"}}\n"
      +"]}";

    checkForMissingFieldsException(schema);

    schema = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"name\", \"type\": [\"null\", \"string\"]}\n"
      +"]}";

    checkForMissingFieldsException(schema);
  }

  private void checkForMissingFieldsException(String schema) throws StageException, IOException {
    Schema avroSchema = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Map<String, Field> employee = new HashMap<>();
    // create empty record with no fields
    record.set(Field.create(employee));
    try {
      AvroTypeUtil.sdcRecordToAvro(record, avroSchema, new HashMap<String, Object>());
      Assert.fail("Expected DataGeneratorException as the field name does not have a default value in the avro " +
        "schema and the record does not contain that field");
    } catch (DataGeneratorException e) {
      Assert.assertEquals(Errors.AVRO_GENERATOR_00, e.getErrorCode());
    }
  }

  @Test
  public void testDefaultsFromSchemaWithTwoArrays() throws Exception {
    // The only important aspect of this schema is that it have two independent arrays. Names and other structures
    // are not important for this test.
    String schema = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"master_record\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"array1\",\n" +
        "    \"type\" : [ \"null\", {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"array1_record\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"field1\",\n" +
        "          \"type\" : \"string\",\n" +
        "          \"default\" : \"a\"\n" +
        "        } ]\n" +
        "      }\n" +
        "    } ],\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"array2\",\n" +
        "    \"type\" : [ \"null\", {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"array2_record\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"field2\",\n" +
        "          \"type\" : \"string\",\n" +
        "          \"default\" : \"b\"\n" +
        "        } ]\n" +
        "      }\n" +
        "    } ],\n" +
        "    \"default\" : null\n" +
        "  } ]\n" +
        "}\n";

    Schema avroSchema = new Schema.Parser().parse(schema);
    Map<String, Object> defaults = AvroTypeUtil.getDefaultValuesFromSchema(avroSchema, new HashSet<String>());
    Assert.assertEquals(4, defaults.size());
    Assert.assertTrue(defaults.containsKey("master_record.array1"));
    Assert.assertTrue(defaults.containsKey("master_record.array2"));
    Assert.assertTrue(defaults.containsKey("array2_record.field2"));
    Assert.assertTrue(defaults.containsKey("array1_record.field1"));
  }

  @Test
  public void testNullValueWithNoDefault() throws IOException {

    // Default values are missing and record has a null value for the field.
    // This should send the record to error.
    String schema = "{ \"type\": \"record\", "+
        "  \"name\": \"NoDefaultValues\"," +
        "  \"fields\" : [{\"name\": \"value\", \"type\": \"string\"}] " +
        "  }" +
        "}";
    Schema parse = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("value", Field.create(Field.Type.STRING, null));
    record.set(Field.create(fields));

    try {
      AvroTypeUtil.sdcRecordToAvro(
          record,
          parse,
          AvroTypeUtil.getDefaultValuesFromSchema(parse, new HashSet<String>())
      );
      Assert.fail();
    } catch (DataGeneratorException e) {
      Assert.assertEquals(Errors.AVRO_GENERATOR_01, e.getErrorCode());
    } catch (StageException e){
      Assert.fail();
    }
  }

  @Test
  public void testNullValueInUnionWithNoDefault() throws IOException {

    // Default values are missing and record has a null value.
    // Since union has a type null, this should succeed.
    String schema = "{ \"type\": \"record\", "+
        "  \"name\": \"NoDefaultValues\"," +
        "  \"fields\" : [" +
        "      {\"name\": \"value\", \"type\": [\"string\", \"null\"]}" +
        "    ]}" +
        "}";
    Schema parse = new Schema.Parser().parse(schema);
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("value", Field.create(Field.Type.STRING, null));
    record.set(Field.create(fields));

    try {
      AvroTypeUtil.sdcRecordToAvro(
          record,
          parse,
          AvroTypeUtil.getDefaultValuesFromSchema(parse, new HashSet<String>())
      );
    } catch (DataGeneratorException e) {
      Assert.fail();
    } catch (StageException e){
      Assert.fail();
    }
  }
}
