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
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SdcAvroTestUtil {

  private static final String AVRO_UNION_TYPE_INDEX_PREFIX = "avro.union.typeIndex.";

  public static final String AVRO_SCHEMA1 = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"Employee\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"name\", \"type\": \"string\"},\n"
    +" {\"name\": \"age\", \"type\": \"int\"},\n"
    +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
    +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
    +"]}";

  public static final String AVRO_SCHEMA2 = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"Employee\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"name\", \"type\": \"string\"},\n"
    +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
    +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
    +"]}";

  public static List<Record> getRecords1() {
    List<Record> records = new ArrayList<>();

    Map<String, Field> bossMap = new HashMap<>();
    bossMap.put("name", Field.create("boss"));
    bossMap.put("age", Field.create(60));
    bossMap.put("emails", Field.create(ImmutableList.of(Field.create("boss@company.com"),
      Field.create("boss2@company.com"))));
    bossMap.put("boss", Field.create(Field.Type.MAP, null));

    Map<String, Field> map = new HashMap<>();
    map.put("name", Field.create("a"));
    map.put("age", Field.create(30));
    map.put("emails", Field.create(ImmutableList.of(Field.create("a@company.com"), Field.create("a2@company.com"))));
    map.put("boss", Field.create(bossMap));
    Record record = RecordCreator.create();
    record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + "/boss", "0");
    record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + "/boss/boss", "1");

    record.set(Field.create(map));
    records.add(record);

    map = new HashMap<>();
    map.put("name", Field.create("b"));
    map.put("age", Field.create(40));
    map.put("emails", Field.create(ImmutableList.of(Field.create("b@company.com"), Field.create("b2@company.com"))));
    map.put("boss", Field.create(bossMap));
    record = RecordCreator.create();
    record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + "/boss", "0");
    record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + "/boss/boss", "1");

    record.set(Field.create(map));
    records.add(record);

    map = new HashMap<>();
    map.put("name", Field.create("c"));
    map.put("age", Field.create(50));
    map.put("emails", Field.create(ImmutableList.of(Field.create("c@company.com"), Field.create("c2@company.com"))));
    map.put("boss", Field.create(bossMap));
    record = RecordCreator.create();
    record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + "/boss", "0");
    record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + "/boss/boss", "1");

    record.set(Field.create(map));
    records.add(record);

    return records;
  }

  public static void compare1(List<GenericRecord> genericRecords) {

    Assert.assertEquals(3, genericRecords.size());

    GenericRecord genericRecord = genericRecords.get(0);
    Assert.assertEquals("a", genericRecord.get("name").toString());
    Assert.assertEquals(30, genericRecord.get("age"));
    Assert.assertTrue(genericRecord.get("emails") instanceof List);
    Assert.assertEquals(2, ((List<Utf8>) genericRecord.get("emails")).size());
    Assert.assertEquals("a@company.com", ((List<Utf8>) genericRecord.get("emails")).get(0).toString());
    Assert.assertEquals("a2@company.com", ((List<Utf8>) genericRecord.get("emails")).get(1).toString());

    GenericRecord boss = (GenericRecord) genericRecord.get("boss");
    Assert.assertNotNull(boss);

    Assert.assertEquals("boss", boss.get("name").toString());
    Assert.assertEquals(60, boss.get("age"));
    Assert.assertTrue(boss.get("emails") instanceof List);
    Assert.assertEquals(2, ((List<Utf8>) boss.get("emails")).size());
    Assert.assertEquals("boss@company.com", ((List<Utf8>) boss.get("emails")).get(0).toString());
    Assert.assertEquals("boss2@company.com", ((List<Utf8>) boss.get("emails")).get(1).toString());

    Assert.assertNull(boss.get("boss"));

    //Record 2
    genericRecord = genericRecords.get(1);
    Assert.assertEquals("b", genericRecord.get("name").toString());
    Assert.assertEquals(40, genericRecord.get("age"));
    Assert.assertTrue(genericRecord.get("emails") instanceof List);
    Assert.assertEquals(2, ((List<Utf8>) genericRecord.get("emails")).size());
    Assert.assertEquals("b@company.com", ((List<Utf8>) genericRecord.get("emails")).get(0).toString());
    Assert.assertEquals("b2@company.com", ((List<Utf8>) genericRecord.get("emails")).get(1).toString());

    boss = (GenericRecord) genericRecord.get("boss");
    Assert.assertNotNull(boss);

    Assert.assertEquals("boss", boss.get("name").toString());
    Assert.assertEquals(60, boss.get("age"));
    Assert.assertTrue(boss.get("emails") instanceof List);
    Assert.assertEquals(2, ((List<Utf8>) boss.get("emails")).size());
    Assert.assertEquals("boss@company.com", ((List<Utf8>) boss.get("emails")).get(0).toString());
    Assert.assertEquals("boss2@company.com", ((List<Utf8>) boss.get("emails")).get(1).toString());

    Assert.assertNull(boss.get("boss"));

    //Record 3
    genericRecord = genericRecords.get(2);
    Assert.assertEquals("c", genericRecord.get("name").toString());
    Assert.assertEquals(50, genericRecord.get("age"));
    Assert.assertTrue(genericRecord.get("emails") instanceof List);
    Assert.assertEquals(2, ((List<Utf8>) genericRecord.get("emails")).size());
    Assert.assertEquals("c@company.com", ((List<Utf8>) genericRecord.get("emails")).get(0).toString());
    Assert.assertEquals("c2@company.com", ((List<Utf8>) genericRecord.get("emails")).get(1).toString());

    boss = (GenericRecord) genericRecord.get("boss");
    Assert.assertNotNull(boss);

    Assert.assertEquals("boss", boss.get("name").toString());
    Assert.assertEquals(60, boss.get("age"));
    Assert.assertTrue(boss.get("emails") instanceof List);
    Assert.assertEquals(2, ((List<Utf8>) boss.get("emails")).size());
    Assert.assertEquals("boss@company.com", ((List<Utf8>) boss.get("emails")).get(0).toString());
    Assert.assertEquals("boss2@company.com", ((List<Utf8>) boss.get("emails")).get(1).toString());

    Assert.assertNull(boss.get("boss"));
  }

  public static List<Record> getRecords2() {

    List<Record> records = new ArrayList<>();

    Map<String, Field> bossMap = new HashMap<>();
    bossMap.put("name", Field.create("boss"));
    bossMap.put("age", Field.create(60));
    bossMap.put("emails", Field.create(ImmutableList.of(Field.create("boss@company.com"),
      Field.create("boss2@company.com"))));
    bossMap.put("boss", Field.create(Field.Type.MAP, null));

    Map<String, Field> map = new HashMap<>();
    map.put("name", Field.create("a"));
    map.put("age", Field.create(30));
    map.put("emails", Field.create(ImmutableList.of(Field.create("a@company.com"), Field.create("a2@company.com"))));
    map.put("boss", Field.create(bossMap));
    Record record = RecordCreator.create();
    record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + "/boss", "1");

    record.set(Field.create(map));
    records.add(record);

    map = new HashMap<>();
    map.put("name", Field.create("b"));
    map.put("age", Field.create(40));
    map.put("emails", Field.create(ImmutableList.of(Field.create("b@company.com"), Field.create("b2@company.com"))));
    map.put("boss", Field.create(bossMap));
    record = RecordCreator.create();
    record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + "/boss", "1");
    record.set(Field.create(map));
    records.add(record);

    map = new HashMap<>();
    map.put("name", Field.create("c"));
    map.put("age", Field.create(50));
    map.put("emails", Field.create(ImmutableList.of(Field.create("c@company.com"), Field.create("c2@company.com"))));
    map.put("boss", Field.create(bossMap));
    record = RecordCreator.create();
    record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + "/boss", "1");
    record.set(Field.create(map));
    records.add(record);
    return records;
  }

  public static void compare2(List<GenericRecord> genericRecords) {

    Assert.assertEquals(3, genericRecords.size());

    //Record 1
    GenericRecord genericRecord = genericRecords.get(0);
    Assert.assertEquals("a", genericRecord.get("name").toString());
    Assert.assertNull(genericRecord.get("age"));
    Assert.assertTrue(genericRecord.get("emails") instanceof List);
    Assert.assertEquals(2, ((List<Utf8>) genericRecord.get("emails")).size());
    Assert.assertEquals("a@company.com", ((List<Utf8>) genericRecord.get("emails")).get(0).toString());
    Assert.assertEquals("a2@company.com", ((List<Utf8>) genericRecord.get("emails")).get(1).toString());

    GenericRecord boss = (GenericRecord) genericRecord.get("boss");
    Assert.assertNull(boss);

    //Record 2
    genericRecord = genericRecords.get(1);
    Assert.assertEquals("b", genericRecord.get("name").toString());
    Assert.assertNull(genericRecord.get("age"));
    Assert.assertTrue(genericRecord.get("emails") instanceof List);
    Assert.assertEquals(2, ((List<Utf8>) genericRecord.get("emails")).size());
    Assert.assertEquals("b@company.com", ((List<Utf8>) genericRecord.get("emails")).get(0).toString());
    Assert.assertEquals("b2@company.com", ((List<Utf8>) genericRecord.get("emails")).get(1).toString());

    boss = (GenericRecord) genericRecord.get("boss");
    Assert.assertNull(boss);

    //Record 3
    genericRecord = genericRecords.get(2);
    Assert.assertEquals("c", genericRecord.get("name").toString());
    Assert.assertNull(genericRecord.get("age"));
    Assert.assertTrue(genericRecord.get("emails") instanceof List);
    Assert.assertEquals(2, ((List<Utf8>) genericRecord.get("emails")).size());
    Assert.assertEquals("c@company.com", ((List<Utf8>) genericRecord.get("emails")).get(0).toString());
    Assert.assertEquals("c2@company.com", ((List<Utf8>) genericRecord.get("emails")).get(1).toString());

    boss = (GenericRecord) genericRecord.get("boss");
    Assert.assertNull(boss);
  }

  private static String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  public static final String AVRO_SCHEMA = "{\n"
    +"\"type\": \"record\",\n"
    +"\"name\": \"Employee\",\n"
    +"\"fields\": [\n"
    +" {\"name\": \"name\", \"type\": \"string\"},\n"
    +" {\"name\": \"age\", \"type\": \"int\"},\n"
    +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
    +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
    +"]}";

  public static File createAvroDataFile() throws Exception {
    File f = new File(createTestDir(), "file-0.avro");
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    GenericRecord boss = new GenericData.Record(schema);
    boss.put("name", "boss");
    boss.put("age", 60);
    boss.put("emails", ImmutableList.of("boss@company.com", "boss2@company.com"));
    boss.put("boss", null);

    GenericRecord e3 = new GenericData.Record(schema);
    e3.put("name", "c");
    e3.put("age", 50);
    e3.put("emails", ImmutableList.of("c@company.com", "c2@company.com"));
    e3.put("boss", boss);

    GenericRecord e2 = new GenericData.Record(schema);
    e2.put("name", "b");
    e2.put("age", 40);
    e2.put("emails", ImmutableList.of("b@company.com", "b2@company.com"));
    e2.put("boss", boss);

    GenericRecord e1 = new GenericData.Record(schema);
    e1.put("name", "a");
    e1.put("age", 30);
    e1.put("emails", ImmutableList.of("a@company.com", "a2@company.com"));
    e1.put("boss", boss);

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, f);
    dataFileWriter.append(e1);
    dataFileWriter.append(e2);
    dataFileWriter.append(e3);

    dataFileWriter.flush();
    dataFileWriter.close();

    return f;
  }
}
