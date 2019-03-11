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
package com.streamsets.pipeline.stage.lib.hive;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.sdk.ElUtil;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import com.streamsets.pipeline.stage.processor.hive.HMPDataFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

public class TestHiveMetastoreUtil {

  private ELVars variables;
  private ELEval eval;

  @Before
  public void setUpEls() {
    this.variables = ElUtil.createELVars();
    this.eval = ElUtil.createElEval("not-important", FieldPathEL.class);
  }

  // Utility function to generate HiveTypeInfo from HiveType.
  public static HiveTypeInfo generatePrimitiveTypeInfo(HiveType type, String comment){
    return type.getSupport().createTypeInfo(type, comment);
  }

  // Utility function to generate HiveTypeInfo from HiveType.
  public static HiveTypeInfo generateDecimalTypeInfo(String comment, int precision, int scale){
    return HiveType.DECIMAL.getSupport().createTypeInfo(HiveType.DECIMAL, comment, precision, scale);
  }

  @Test
  public void testConvertRecordToHMSTypeSimple() {
    // Test for types that don't require type conversion
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("string", Field.create("2016-05-27"));
    map.put("boolean", Field.create(true));
    map.put("integer", Field.create(12345));
    map.put("negative", Field.create(-12345));
    map.put("long", Field.create(100000L));
    map.put("float", Field.create(12.5f));
    map.put("double", Field.create(1232345891.4d));
    record.set(Field.create(map));

    Map<String, HiveTypeInfo> expected = new LinkedHashMap<>();
    expected.put("string", generatePrimitiveTypeInfo(HiveType.STRING, "string"));
    expected.put("boolean", generatePrimitiveTypeInfo(HiveType.BOOLEAN, "boolean"));
    expected.put("integer", generatePrimitiveTypeInfo(HiveType.INT, "integer"));
    expected.put("negative", generatePrimitiveTypeInfo(HiveType.INT, "negative"));
    expected.put("long", generatePrimitiveTypeInfo(HiveType.BIGINT,"long"));
    expected.put("float", generatePrimitiveTypeInfo(HiveType.FLOAT, "float"));
    expected.put("double", generatePrimitiveTypeInfo(HiveType.DOUBLE, "double"));

    Map<String, HiveTypeInfo> actual = null;
    try {
      actual = HiveMetastoreUtil.convertRecordToHMSType(
        record,
        eval,
        eval,
        eval,
        "",
        "",
        "${field:field()}",
        variables,
        false,
        TimeZone.getDefault()
      );
    } catch (StageException e){
      Assert.fail("convertRecordToHMSType threw StageException:" + e.getMessage());
    }
    Assert.assertNotNull(actual);
    for(Map.Entry<String, HiveTypeInfo> pair:  expected.entrySet()) {
      HiveTypeInfo actualType = actual.get(pair.getKey());
      Assert.assertEquals(pair.getValue().getHiveType(), actualType.getHiveType());
      Assert.assertEquals(pair.getValue().getComment(), actualType.getComment());
    }
  }

  @Test(expected = HiveStageCheckedException.class)
  public void testConvertRecordToHMSTypeIncorrectRootType() throws Exception {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST, Collections.emptyList()));

    HiveMetastoreUtil.convertRecordToHMSType(
        record,
        eval,
        eval,
        eval,
        "",
        "",
        "",
        variables,
        false,
        TimeZone.getDefault()
    );
  }

  @Test
  public void testConvertRecordToHMSTypeConvert() {
    /* Test for types that require special handling
        [SDC record]   [HiveType after convertRecordToHMSType call]
        char             -> string
        short            -> int
        date             -> string
        decimal          -> decimal
    */
    // Sample data
    Date today = new Date();
    BigDecimal decimalVal =  new BigDecimal(1234567889);

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("char", Field.create('a'));
    map.put("short", Field.create((short)10));
    map.put("date", Field.createDate(today));
    map.put("decimal", Field.create(decimalVal));
    record.set(Field.create(map));

    Map<String, HiveTypeInfo> expected = new LinkedHashMap<>();
    expected.put("char", generatePrimitiveTypeInfo(HiveType.STRING, "char"));
    expected.put("short", generatePrimitiveTypeInfo(HiveType.INT, "short"));
    expected.put("date", generatePrimitiveTypeInfo(HiveType.DATE, "date"));
    expected.put("decimal", generateDecimalTypeInfo("decimal", decimalVal.scale(), decimalVal.precision()));

    Map<String, HiveTypeInfo> actual = null;
    try {
      actual = HiveMetastoreUtil.convertRecordToHMSType(
          record,
          eval,
          eval,
          eval,
          String.valueOf(decimalVal.scale()),
          String.valueOf(decimalVal.precision()),
          "${field:field()}",
          variables,
          false,
          TimeZone.getDefault()
      );
    } catch (StageException e){
      Assert.fail("convertRecordToHMSType threw StageException:" + e.getMessage());
    }
    Assert.assertNotNull(actual);
    // Test if all the HiveTypeInfo has the same HiveType as expected
    for(Map.Entry<String, HiveTypeInfo> pair:  expected.entrySet()) {
      HiveTypeInfo actualType = actual.get(pair.getKey());
      Assert.assertEquals(pair.getValue().getHiveType(), actualType.getHiveType());
      Assert.assertEquals(pair.getValue().getComment(), actualType.getComment());
    }

    // Test if the Field type and values in original Record are converted correctly
    Map<String, Field> list = record.get().getValueAsListMap();
    Field f1 = list.get("char");
    Assert.assertEquals(f1.getType(), Field.Type.STRING);
    Assert.assertTrue(f1.getValue() instanceof String);
    Assert.assertEquals(f1.getValueAsChar(), 'a');

    Field f2 = list.get("short");
    Assert.assertEquals(f2.getType(), Field.Type.INTEGER);
    Assert.assertTrue(f2.getValue() instanceof Integer);
    Assert.assertEquals(f2.getValueAsInteger(), 10);

    Field f3 = list.get("date");
    Assert.assertEquals(f3.getType(), Field.Type.DATE);
    Assert.assertTrue(f3.getValue() instanceof Date);
    Assert.assertEquals(f3.getValueAsString(), today.toString());

    Field f4 = list.get("decimal");
    Assert.assertEquals(f4.getType(), Field.Type.DECIMAL);
    Assert.assertTrue(f4.getValue() instanceof BigDecimal);
    Assert.assertEquals(f4.getValueAsDecimal().scale(), decimalVal.scale());
    Assert.assertEquals(f4.getValueAsDecimal().precision(), decimalVal.precision());
    Assert.assertEquals(f4.getValueAsDecimal().toString(), decimalVal.toString());
  }

  @Test
  public void testUnsupportedColumnValue() {
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("/"));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("*"));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("="));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("%"));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("\\"));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("^"));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("[]"));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("?"));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("'"));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("\""));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("123=45"));
    Assert.assertTrue(HiveMetastoreUtil.hasUnsupportedChar("test[3]"));

    // They don't contain unsupported chars
    Assert.assertFalse(HiveMetastoreUtil.hasUnsupportedChar("$56.77"));
    Assert.assertFalse(HiveMetastoreUtil.hasUnsupportedChar("2016-01-01"));
    Assert.assertFalse(HiveMetastoreUtil.hasUnsupportedChar("hive_avro+hdfs"));
    Assert.assertFalse(HiveMetastoreUtil.hasUnsupportedChar("test(3)"));
  }

  @Test
  public void testValidateColumnName() {
    Assert.assertTrue(HiveMetastoreUtil.validateObjectName("table"));
    Assert.assertTrue(HiveMetastoreUtil.validateObjectName("Jarcec"));
    Assert.assertTrue(HiveMetastoreUtil.validateObjectName("Junko"));
    Assert.assertTrue(HiveMetastoreUtil.validateObjectName("Santhosh"));
    Assert.assertTrue(HiveMetastoreUtil.validateObjectName("sdc_log"));
    Assert.assertTrue(HiveMetastoreUtil.validateObjectName("sdc_log2"));
    Assert.assertTrue(HiveMetastoreUtil.validateObjectName("_abc"));

    Assert.assertFalse(HiveMetastoreUtil.validateObjectName("cool column"));
    Assert.assertFalse(HiveMetastoreUtil.validateObjectName("0"));
    Assert.assertFalse(HiveMetastoreUtil.validateObjectName("cool@column"));
  }

  @Test
  public void testStripHDFSHostPort() throws Exception{
    Assert.assertEquals("/usr/hive", HiveMetastoreUtil.stripHdfsHostAndPort("hdfs://host/usr/hive"));
    Assert.assertEquals("/usr", HiveMetastoreUtil.stripHdfsHostAndPort("hdfs://host:4567/usr"));
    Assert.assertEquals("/", HiveMetastoreUtil.stripHdfsHostAndPort("hdfs://host:4567/"));
    Assert.assertEquals("/", HiveMetastoreUtil.stripHdfsHostAndPort("hdfs://host/"));

    try {
      HiveMetastoreUtil.stripHdfsHostAndPort(null);
      Assert.fail("Should fail if location is null");
    } catch (NullPointerException e) {
      //Expected
    }

    try {
      HiveMetastoreUtil.stripHdfsHostAndPort("");
      Assert.fail("Should fail if location is empty");
    } catch (IllegalArgumentException e) {
      //Expected
    }
  }

  @Test
  public void testOldVersionDefault() throws Exception {
    final int version = 1;
    final String dataFormat = "Parquet";

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("version", Field.create(version));
    map.put("dataFormat", Field.create(dataFormat));
    record.set(Field.create(map));

    Assert.assertEquals(HiveMetastoreUtil.getDataFormat(record), HMPDataFormat.AVRO.getLabel());
  }

  @Test
  public void testEmptyDataFormatForNewVersion() throws Exception {
    final int version = 2;
    final String dataFormat = "Text";

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("version", Field.create(version));
    record.set(Field.create(map));


    try {
      HiveMetastoreUtil.getDataFormat(record);
      Assert.fail("Should fail for unsupported dataFormat: '" + dataFormat + "'");
    } catch (HiveStageCheckedException ex) {
      // expected
    }

  }

}
