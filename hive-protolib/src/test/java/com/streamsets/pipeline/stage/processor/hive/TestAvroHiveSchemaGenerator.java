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
package com.streamsets.pipeline.stage.processor.hive;

import com.streamsets.pipeline.stage.lib.hive.AvroHiveSchemaGenerator;
import com.streamsets.pipeline.stage.lib.hive.typesupport.*;
import com.streamsets.pipeline.stage.lib.hive.TestHiveMetastoreUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class TestAvroHiveSchemaGenerator {

  @Test
  public void testGenerateSimpleSchema() throws Exception {
    // Incoming record structure: Map<String, HiveTypeInfo>
    //  intColumn: Int
    //  strColumn: String
    //  boolColumn: Boolean
    String expected = "{\"type\":\"record\"," +
        "\"name\":\"test\"," +
        "\"fields\":[" +
        "{\"name\":\"intColumn\",\"type\":[\"null\",\"int\"],\"default\":null}," +
        "{\"name\":\"strColumn\",\"type\":[\"null\",\"string\"],\"default\":null}," +
        "{\"name\":\"boolColumn\",\"type\":[\"null\",\"boolean\"],\"default\":null}]" +
        "}";

    Map<String, HiveTypeInfo> record = new LinkedHashMap<>();
    record.put("intColumn", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT, "intColumn")) ;
    record.put("strColumn", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "strColumn"));
    record.put("boolColumn", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.BOOLEAN, "boolColumn"));

    AvroHiveSchemaGenerator gen = new AvroHiveSchemaGenerator("test");
    String result = gen.inferSchema(record);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, expected);
  }

  @Test
  public void testGenerateSchemaCharSmallInt() throws Exception {
    // Incoming record structure: Map<String, HiveTypeInfo>
    //  first: String
    //  second: String
    //  third: Integer
    String expected = "{\"type\":\"record\"," +
        "\"name\":\"test\"," +
        "\"fields\":[" +
        "{\"name\":\"first\",\"type\":[\"null\",\"string\"],\"default\":null}," +
        "{\"name\":\"second\",\"type\":[\"null\",\"string\"],\"default\":null}," +
        "{\"name\":\"third\",\"type\":[\"null\",\"int\"],\"default\":null}]" +
        "}";
    Map<String, HiveTypeInfo> record = new LinkedHashMap<>();
    record.put("first", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "first")) ;
    record.put("second", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "second"));
    record.put("third", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.INT, "third"));

    AvroHiveSchemaGenerator gen = new AvroHiveSchemaGenerator("test");
    String result = gen.inferSchema(record);
    Assert.assertEquals(result, expected);
  }

  @Test
  public void testGenerateSchemaBigintFloatDouble() throws Exception {
    // Incoming record structure: Map<String, HiveTypeInfo>
    //  first: bigint
    //  second: float
    //  third: double
    String expected = "{\"type\":\"record\"," +
        "\"name\":\"test\"," +
        "\"fields\":[" +
        "{\"name\":\"first\",\"type\":[\"null\",\"long\"],\"default\":null}," +
        "{\"name\":\"second\",\"type\":[\"null\",\"float\"],\"default\":null}," +
        "{\"name\":\"third\",\"type\":[\"null\",\"double\"],\"default\":null}]" +
        "}";
    Map<String, HiveTypeInfo> record = new LinkedHashMap<>();
    record.put("first", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.BIGINT, "first")) ;
    record.put("second", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.FLOAT, "second"));
    record.put("third", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.DOUBLE, "double"));

    AvroHiveSchemaGenerator gen = new AvroHiveSchemaGenerator("test");
    String result = gen.inferSchema(record);
    Assert.assertEquals(result, expected);
  }

  @Test
  public void testGenerateSchemaDateByte() throws Exception {
    // Incoming record structure: Map<String, HiveTypeInfo>
    //  first: date
    //  second: byte

    String expected = "{\"type\":\"record\"," +
        "\"name\":\"test\"," +
        "\"fields\":[" +
        "{\"name\":\"first\",\"type\":[\"null\",\"string\"],\"default\":null}," +
        "{\"name\":\"second\",\"type\":[\"null\",\"bytes\"],\"default\":null}]" +
        "}";
    Map<String, HiveTypeInfo> record = new LinkedHashMap<>();
    record.put("first", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "first"));
    record.put("second", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.BINARY, "second"));

    AvroHiveSchemaGenerator gen = new AvroHiveSchemaGenerator("test");
    String result = gen.inferSchema(record);
    Assert.assertEquals(result, expected);
  }

  @Test
  public void testGenerateSchemaDecimal() throws Exception {
    // Incoming record structure: Map<String, HiveTypeInfo>
    //  first: decimal (precision 2, scale 1)
    //  second: string

    String expected = "{\"type\":\"record\"," +
        "\"name\":\"test\"," +
        "\"fields\":[" +
        "{\"name\":\"first\"," + "\"type\":" +
        "[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":2,\"scale\":1}],\"default\":null}," +
        "{\"name\":\"second\",\"type\":[\"null\",\"string\"],\"default\":null}]" +
        "}";
    Map<String, HiveTypeInfo> record = new LinkedHashMap<>();
    record.put("first", TestHiveMetastoreUtil.generateDecimalTypeInfo("first",2, 1)) ;
    record.put("second", TestHiveMetastoreUtil.generatePrimitiveTypeInfo(HiveType.STRING, "second"));

    AvroHiveSchemaGenerator gen = new AvroHiveSchemaGenerator("test");
    String result = gen.inferSchema(record);
    Assert.assertEquals(result, expected);
  }

}
