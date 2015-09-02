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
package com.streamsets.pipeline.api;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field.Type;
import com.streamsets.pipeline.api.impl.BooleanTypeSupport;
import com.streamsets.pipeline.api.impl.ByteArrayTypeSupport;
import com.streamsets.pipeline.api.impl.ByteTypeSupport;
import com.streamsets.pipeline.api.impl.CharTypeSupport;
import com.streamsets.pipeline.api.impl.DateTypeSupport;
import com.streamsets.pipeline.api.impl.DecimalTypeSupport;
import com.streamsets.pipeline.api.impl.DoubleTypeSupport;
import com.streamsets.pipeline.api.impl.FloatTypeSupport;
import com.streamsets.pipeline.api.impl.IntegerTypeSupport;
import com.streamsets.pipeline.api.impl.LongTypeSupport;
import com.streamsets.pipeline.api.impl.ShortTypeSupport;
import com.streamsets.pipeline.api.impl.StringTypeSupport;
import com.streamsets.pipeline.api.impl.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestField {

  @Test
  public void testTypeSupporters() {
    Assert.assertEquals(BooleanTypeSupport.class, Type.BOOLEAN.supporter.getClass());
    Assert.assertEquals(CharTypeSupport.class, Type.CHAR.supporter.getClass());
    Assert.assertEquals(ByteTypeSupport.class, Type.BYTE.supporter.getClass());
    Assert.assertEquals(ShortTypeSupport.class, Type.SHORT.supporter.getClass());
    Assert.assertEquals(IntegerTypeSupport.class, Type.INTEGER.supporter.getClass());
    Assert.assertEquals(LongTypeSupport.class, Type.LONG.supporter.getClass());
    Assert.assertEquals(FloatTypeSupport.class, Type.FLOAT.supporter.getClass());
    Assert.assertEquals(DoubleTypeSupport.class, Type.DOUBLE.supporter.getClass());
    Assert.assertEquals(DecimalTypeSupport.class, Type.DECIMAL.supporter.getClass());
    Assert.assertEquals(DateTypeSupport.class, Type.DATE.supporter.getClass());
    Assert.assertEquals(DateTypeSupport.class, Type.DATETIME.supporter.getClass());
    Assert.assertEquals(StringTypeSupport.class, Type.STRING.supporter.getClass());
    Assert.assertEquals(ByteArrayTypeSupport.class, Type.BYTE_ARRAY.supporter.getClass());
  }

  @Test
  public void testCreateInferringTypeFromValue() {
    Field f = Field.create(true);
    Assert.assertEquals(Type.BOOLEAN, f.getType());
    Assert.assertEquals(true, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create('c');
    Assert.assertEquals(Type.CHAR, f.getType());
    Assert.assertEquals('c', f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create((byte) 1);
    Assert.assertEquals(Type.BYTE, f.getType());
    Assert.assertEquals((byte) 1, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create((short) 2);
    Assert.assertEquals(Type.SHORT, f.getType());
    Assert.assertEquals((short) 2, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create(3);
    Assert.assertEquals(Type.INTEGER, f.getType());
    Assert.assertEquals(3, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create((long) 4);
    Assert.assertEquals(Type.LONG, f.getType());
    Assert.assertEquals((long) 4, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create((float) 5);
    Assert.assertEquals(Type.FLOAT, f.getType());
    Assert.assertEquals((float) 5, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create((double) 6);
    Assert.assertEquals(Type.DOUBLE, f.getType());
    Assert.assertEquals((double) 6, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create(new BigDecimal(1));
    Assert.assertEquals(Type.DECIMAL, f.getType());
    Assert.assertEquals(new BigDecimal(1), f.getValue());
    Assert.assertNotNull(f.toString());

    Date d = new Date();
    f = Field.createDate(d);
    Assert.assertEquals(Type.DATE, f.getType());
    Assert.assertEquals(d, f.getValue());
    Assert.assertNotSame(d, f.getValue());
    Assert.assertNotNull(f.toString());

    d = new Date();
    f = Field.createDatetime(d);
    Assert.assertEquals(Type.DATETIME, f.getType());
    Assert.assertEquals(d, f.getValue());
    Assert.assertNotSame(d, f.getValue());
    Assert.assertNotNull(f.toString());

    String s = "s";
    f = Field.create(s);
    Assert.assertEquals(Type.STRING, f.getType());
    Assert.assertEquals(s, f.getValue());
    Assert.assertSame(s, f.getValue());
    Assert.assertNotNull(f.toString());

    byte[] array = new byte[1];
    array[0] = 7;
    f = Field.create(array);
    Assert.assertEquals(Type.BYTE_ARRAY, f.getType());
    Assert.assertArrayEquals(array, (byte[]) f.getValue());
    Assert.assertNotSame(array, f.getValue());
    Assert.assertNotNull(f.toString());
  }

  @Test(expected = NullPointerException.class)
  public void testCreateWithNullType() {
    Field.create((Type) null, null);
  }

  @Test
  public void testCreateWithTypeAndNullValue() {
    Field f = Field.create(Type.BOOLEAN, null);
    Assert.assertEquals(Type.BOOLEAN, f.getType());
    Assert.assertNull(f.getValue());
    Assert.assertNotNull(f.toString());
  }

  @Test
  public void testCreateWithTypeAndNotNullValue() {
    Field f = Field.create(Type.BOOLEAN, true);
    Assert.assertEquals(Type.BOOLEAN, f.getType());
    Assert.assertEquals(true, f.getValue());
    Assert.assertNotNull(f.toString());
  }

  @Test
  public void testCreateInferringTypeFromOtherField() {
    Field f = Field.create(Type.BOOLEAN, true);
    f = Field.create(f, false);
    Assert.assertEquals(Type.BOOLEAN, f.getType());
    Assert.assertEquals(false, f.getValue());
    Assert.assertNotNull(f.toString());
  }

  @Test
  @SuppressWarnings("ObjectEqualsNull")
  public void testHashCodeEquals() {
    Field f0 = Field.create(Type.STRING, null);
    Field f1 = Field.create(Type.STRING, "a");
    Field f2 = Field.create(Type.STRING, "a");
    Field f3 = Field.create(Type.STRING, "b");
    Assert.assertEquals(0, f0.hashCode());
    Assert.assertNotEquals(f0.hashCode(), f1.hashCode());
    Assert.assertEquals(f1.hashCode(), f2.hashCode());
    Assert.assertNotEquals(f1.hashCode(), f3.hashCode());
    Assert.assertFalse(f0.equals(null));
    Assert.assertFalse(f0.equals(new Object()));
    Assert.assertFalse(f0.equals(f1));
    Assert.assertTrue(f1.equals(f1));
    Assert.assertTrue(f1.equals(f2));
    Assert.assertTrue(f2.equals(f1));
    Assert.assertFalse(f1.equals(f3));
    Assert.assertFalse(f3.equals(f1));
  }

  @Test
  public void testEqualsWithByteArray() {
    Field f0 = Field.create(new byte[] { 0, 1, 2 });
    Field f1 = Field.create(new byte[] { 0, 1, 2 });
    Field f2 = Field.create(Type.BYTE_ARRAY, null);
    Field f3 = Field.create(new byte[] { 1, 1, 2 });
    Assert.assertTrue(f0.equals(f1));
    Assert.assertFalse(f0.equals(f2));
    Assert.assertFalse(f0.equals(f3));
  }

  private static final Date DATE_VALUE = new Date(System.currentTimeMillis() + 100);
  private static final Date DATETIME_VALUE = new Date(System.currentTimeMillis() - 100);

  private static final List<Field> FIELDS = ImmutableList.of(
      Field.create(true),
      Field.create('c'),
      Field.create((byte) 1),
      Field.create((short) 2),
      Field.create((int) 3),
      Field.create((long) 4),
      Field.create((float) 5.1),
      Field.create((double) 6.2),
      Field.createDate(DATE_VALUE),
      Field.createDatetime(DATETIME_VALUE),
      Field.create(new BigDecimal(7.3)),
      Field.create("s"),
      Field.create(new byte[]{1, 2}),
      Field.create(new LinkedHashMap<String, Field>()),
      Field.create(new ArrayList<Field>()),
      Field.createListMap(new LinkedHashMap<String, Field>())
  );

  @SuppressWarnings("unchecked")
  private static final Map<Type, List<Type>> VALID_VALUE_AS = new ImmutableMap.Builder()
      .put(Type.BOOLEAN, ImmutableList.of(Type.BOOLEAN, Type.STRING))
      .put(Type.CHAR, ImmutableList.of(Type.CHAR, Type.STRING))
      .put(Type.BYTE, ImmutableList.of(Type.BOOLEAN, Type.BYTE, Type.STRING, Type.SHORT, Type.INTEGER, Type.LONG,
                                       Type.FLOAT, Type.DOUBLE, Type.DECIMAL))
      .put(Type.SHORT, ImmutableList.of(Type.BOOLEAN, Type.BYTE, Type.STRING, Type.SHORT, Type.INTEGER, Type.LONG,
                                        Type.FLOAT, Type.DOUBLE, Type.DECIMAL))
      .put(Type.INTEGER, ImmutableList.of(Type.BOOLEAN, Type.BYTE, Type.STRING, Type.SHORT, Type.INTEGER, Type.LONG,
                                          Type.FLOAT, Type.DOUBLE, Type.DECIMAL))
      .put(Type.LONG, ImmutableList.of(Type.BOOLEAN, Type.BYTE, Type.STRING, Type.SHORT, Type.INTEGER, Type.LONG,
                                       Type.FLOAT, Type.DOUBLE, Type.DECIMAL, Type.DATE, Type.DATETIME))
      .put(Type.FLOAT, ImmutableList.of(Type.BOOLEAN, Type.BYTE, Type.STRING, Type.SHORT, Type.INTEGER, Type.LONG,
                                        Type.FLOAT, Type.DOUBLE, Type.DECIMAL))
      .put(Type.DOUBLE, ImmutableList.of(Type.BOOLEAN, Type.BYTE, Type.STRING, Type.SHORT, Type.INTEGER, Type.LONG,
                                         Type.FLOAT, Type.DOUBLE, Type.DECIMAL))
      .put(Type.DECIMAL, ImmutableList.of(Type.BOOLEAN, Type.BYTE, Type.STRING, Type.SHORT, Type.INTEGER, Type.LONG,
                                          Type.FLOAT, Type.DOUBLE, Type.DECIMAL))
      .put(Type.STRING, ImmutableList.of(Type.BOOLEAN, Type.BYTE, Type.STRING, Type.SHORT, Type.INTEGER, Type.LONG,
                                         Type.FLOAT, Type.DOUBLE, Type.DECIMAL, Type.DATE, Type.DATETIME, Type.CHAR))
      .put(Type.BYTE_ARRAY, ImmutableList.of(Type.BYTE_ARRAY))
      .put(Type.MAP, ImmutableList.of(Type.MAP, Type.LIST_MAP))
      .put(Type.LIST, ImmutableList.of(Type.LIST, Type.LIST_MAP))
      .put(Type.LIST_MAP, ImmutableList.of(Type.LIST_MAP, Type.MAP, Type.LIST))
      .put(Type.DATE, ImmutableList.of(Type.DATE, Type.DATETIME, Type.STRING, Type.LONG))
      .put(Type.DATETIME, ImmutableList.of(Type.DATE, Type.DATETIME, Type.STRING, Type.LONG))
      .build();

  @Test
  public void testGetValueAs() {
    for (Field f : FIELDS) {
      for (Type t : Field.Type.values()) {
        if (VALID_VALUE_AS.get(f.getType()).contains(t)) {
          switch (t) {
            case BOOLEAN:
              if (f.getType() != Type.STRING) {
                Assert.assertEquals(true, f.getValueAsBoolean());
              } else {
                  Assert.assertEquals(Boolean.valueOf("s"), f.getValueAsBoolean());
              }
              break;
            case CHAR:
              if (f.getType() != Type.STRING) {
                Assert.assertEquals('c', f.getValueAsChar());
              } else {
                Assert.assertEquals("s".charAt(0), f.getValueAsChar());
              }
              break;
            case BYTE:
              switch (f.getType()) {
                case BOOLEAN:
                  Assert.assertEquals((byte) 1, f.getValueAsByte());
                  break;
                case BYTE:
                  Assert.assertEquals((byte) 1, f.getValueAsByte());
                  break;
                case SHORT:
                  Assert.assertEquals((byte) 2, f.getValueAsByte());
                  break;
                case INTEGER:
                  Assert.assertEquals((byte) 3, f.getValueAsByte());
                  break;
                case LONG:
                  Assert.assertEquals((byte) 4, f.getValueAsByte());
                  break;
                case FLOAT:
                  Assert.assertEquals((byte) 5, f.getValueAsByte());
                  break;
                case DOUBLE:
                  Assert.assertEquals((byte) 6, f.getValueAsByte());
                  break;
                case DECIMAL:
                  Assert.assertEquals((byte) 7, f.getValueAsByte());
                  break;
              }
              break;
            case SHORT:
              switch (f.getType()) {
                case BOOLEAN:
                  Assert.assertEquals((short) 1, f.getValueAsShort());
                  break;
                case BYTE:
                  Assert.assertEquals((short) 1, f.getValueAsShort());
                  break;
                case SHORT:
                  Assert.assertEquals((short) 2, f.getValueAsShort());
                  break;
                case INTEGER:
                  Assert.assertEquals((short) 3, f.getValueAsShort());
                  break;
                case LONG:
                  Assert.assertEquals((short) 4, f.getValueAsShort());
                  break;
                case FLOAT:
                  Assert.assertEquals((short) 5, f.getValueAsShort());
                  break;
                case DOUBLE:
                  Assert.assertEquals((short) 6, f.getValueAsShort());
                  break;
                case DECIMAL:
                  Assert.assertEquals((short) 7, f.getValueAsShort());
                  break;
              }
              break;
            case INTEGER:
              switch (f.getType()) {
                case BOOLEAN:
                  Assert.assertEquals((int) 1, f.getValueAsInteger());
                  break;
                case BYTE:
                  Assert.assertEquals((int) 1, f.getValueAsInteger());
                  break;
                case SHORT:
                  Assert.assertEquals((int) 2, f.getValueAsInteger());
                  break;
                case INTEGER:
                  Assert.assertEquals((int) 3, f.getValueAsInteger());
                  break;
                case LONG:
                  Assert.assertEquals((int) 4, f.getValueAsInteger());
                  break;
                case FLOAT:
                  Assert.assertEquals((int) 5, f.getValueAsInteger());
                  break;
                case DOUBLE:
                  Assert.assertEquals((int) 6, f.getValueAsInteger());
                  break;
                case DECIMAL:
                  Assert.assertEquals((int) 7, f.getValueAsInteger());
                  break;
              }
              break;
            case LONG:
              switch (f.getType()) {
                case BOOLEAN:
                  Assert.assertEquals((long) 1, f.getValueAsLong());
                  break;
                case BYTE:
                  Assert.assertEquals((long) 1, f.getValueAsLong());
                  break;
                case SHORT:
                  Assert.assertEquals((long) 2, f.getValueAsLong());
                  break;
                case INTEGER:
                  Assert.assertEquals((long) 3, f.getValueAsLong());
                  break;
                case LONG:
                  Assert.assertEquals((long) 4, f.getValueAsLong());
                  break;
                case FLOAT:
                  Assert.assertEquals((long) 5, f.getValueAsLong());
                  break;
                case DOUBLE:
                  Assert.assertEquals((long) 6, f.getValueAsLong());
                  break;
                case DECIMAL:
                  Assert.assertEquals((long) 7, f.getValueAsLong());
                  break;
              }
              break;
            case FLOAT:
              switch (f.getType()) {
                case BOOLEAN:
                  Assert.assertEquals((float) 1, f.getValueAsFloat(), 0.1);
                  break;
                case BYTE:
                  Assert.assertEquals((float) 1, f.getValueAsFloat(), 0.1);
                  break;
                case SHORT:
                  Assert.assertEquals((float) 2, f.getValueAsFloat(), 0.1);
                  break;
                case INTEGER:
                  Assert.assertEquals((float) 3, f.getValueAsFloat(), 0.1);
                  break;
                case LONG:
                  Assert.assertEquals((float) 4, f.getValueAsFloat(), 0.1);
                  break;
                case FLOAT:
                  Assert.assertEquals((float) 5.1, f.getValueAsFloat(), 0.1);
                  break;
                case DOUBLE:
                  Assert.assertEquals((float) 6.2, f.getValueAsFloat(), 0.1);
                  break;
                case DECIMAL:
                  Assert.assertEquals((float) 7.3, f.getValueAsFloat(), 0.1);
                  break;
              }
              break;
            case DOUBLE:
              switch (f.getType()) {
                case BOOLEAN:
                  Assert.assertEquals((double) 1, f.getValueAsDouble(), 0.1);
                  break;
                case BYTE:
                  Assert.assertEquals((double) 1, f.getValueAsDouble(), 0.1);
                  break;
                case SHORT:
                  Assert.assertEquals((double) 2, f.getValueAsDouble(), 0.1);
                  break;
                case INTEGER:
                  Assert.assertEquals((double) 3, f.getValueAsDouble(), 0.1);
                  break;
                case LONG:
                  Assert.assertEquals((double) 4, f.getValueAsDouble(), 0.1);
                  break;
                case FLOAT:
                  Assert.assertEquals((double) 5.1, f.getValueAsDouble(), 0.1);
                  break;
                case DOUBLE:
                  Assert.assertEquals((double) 6.2, f.getValueAsDouble(), 0.1);
                  break;
                case DECIMAL:
                  Assert.assertEquals((double) 7.3, f.getValueAsDouble(), 0.1);
                  break;
              }
              break;
            case DATE:
              switch (f.getType()) {
                case DATE:
                  Assert.assertEquals(DATE_VALUE, f.getValueAsDate());
                  break;
                case DATETIME:
                  Assert.assertEquals(DATETIME_VALUE, f.getValueAsDate());
                  break;
              }
              break;
            case DATETIME:
              switch (f.getType()) {
                case DATE:
                  Assert.assertEquals(DATE_VALUE, f.getValueAsDatetime());
                  break;
                case DATETIME:
                  Assert.assertEquals(DATETIME_VALUE, f.getValueAsDatetime());
                  break;
              }
              break;
            case DECIMAL:
              switch (f.getType()) {
                case BOOLEAN:
                  Assert.assertEquals(new BigDecimal(1), f.getValueAsDecimal());
                  break;
                case BYTE:
                  Assert.assertEquals(new BigDecimal(1), f.getValueAsDecimal());
                  break;
                case SHORT:
                  Assert.assertEquals(new BigDecimal(2), f.getValueAsDecimal());
                  break;
                case INTEGER:
                  Assert.assertEquals(new BigDecimal(3), f.getValueAsDecimal());
                  break;
                case LONG:
                  Assert.assertEquals(new BigDecimal(4), f.getValueAsDecimal());
                  break;
                case FLOAT:
                  Assert.assertEquals(new BigDecimal(5.1).floatValue(), f.getValueAsDecimal().floatValue(), 0.1);
                  break;
                case DOUBLE:
                  Assert.assertEquals(new BigDecimal(6.2).doubleValue(), f.getValueAsDecimal().doubleValue(), 0.1);
                  break;
                case DECIMAL:
                  Assert.assertEquals(new BigDecimal(7.3), f.getValueAsDecimal());
                  break;
              }
              break;
            case STRING:
              Assert.assertEquals(f.getValue().toString(), f.getValueAsString());
              break;
            case BYTE_ARRAY:
              Assert.assertArrayEquals(new byte[]{1, 2}, f.getValueAsByteArray());
              break;
            case MAP:
              Assert.assertEquals(new LinkedHashMap<String, Field>(), f.getValueAsMap());
              break;
            case LIST:
              Assert.assertEquals(new ArrayList<Field>(), f.getValueAsList());
              break;
            case LIST_MAP:
              Assert.assertEquals(new LinkedHashMap<String, Field>(), f.getValueAsListMap());
              break;
          }
        } else {
          try {
            switch (t) {
              case BOOLEAN:
                f.getValueAsBoolean();
                break;
              case CHAR:
                f.getValueAsChar();
                break;
              case BYTE:
                f.getValueAsByte();
                break;
              case SHORT:
                f.getValueAsShort();
                break;
              case INTEGER:
                f.getValueAsInteger();
                break;
              case LONG:
                f.getValueAsLong();
                break;
              case FLOAT:
                f.getValueAsFloat();
                break;
              case DOUBLE:
                f.getValueAsDouble();
                break;
              case DATE:
                f.getValueAsDate();
                break;
              case DATETIME:
                f.getValueAsDatetime();
                break;
              case DECIMAL:
                f.getValueAsDecimal();
                break;
              case STRING:
                f.getValueAsString();
                break;
              case BYTE_ARRAY:
                f.getValueAsByteArray();
                break;
              case MAP:
                f.getValueAsMap();
                break;
              case LIST:
                f.getValueAsList();
                break;
              case LIST_MAP:
                f.getValueAsListMap();
                break;
            }
            Assert.fail(Utils.format("Failed asserting that type '{}' cannot be get as a '{}'", f.getType(), t));
          } catch (IllegalArgumentException ex) {
            // expected
          }
        }
      }
    }
  }


  @Test
  public void testListMap() {
    LinkedHashMap<String, Field> linkedHashMap = new LinkedHashMap<>();
    linkedHashMap.put("firstField", Field.create("sampleValue"));
    linkedHashMap.put("secondField", Field.create(20));

    Field listMapField = Field.createListMap(linkedHashMap);

    LinkedHashMap<String, Field> linkedHashMapReturnVal = listMapField.getValueAsListMap();
    Assert.assertEquals(2, linkedHashMapReturnVal.size());
    Field firstField = linkedHashMapReturnVal.get("firstField");
    Assert.assertEquals("sampleValue", firstField.getValue());
    Field secondField = linkedHashMapReturnVal.get("secondField");
    Assert.assertEquals(20, secondField.getValue());

    List<Field> list = listMapField.getValueAsList();
    Assert.assertEquals(2, list.size());
    firstField = list.get(0);
    Assert.assertEquals("sampleValue", firstField.getValue());
    secondField = list.get(1);
    Assert.assertEquals(20, secondField.getValue());

    Map<String, Field> map = listMapField.getValueAsMap();
    Assert.assertEquals(2, map.size());
    firstField = linkedHashMapReturnVal.get("firstField");
    Assert.assertEquals("sampleValue", firstField.getValue());
    secondField = linkedHashMapReturnVal.get("secondField");
    Assert.assertEquals(20, secondField.getValue());

    //Test List to ListMap
    List<Field> list1 = new ArrayList<>(3);
    list1.add(Field.create("stringValue"));
    list1.add(Field.create(40));
    list1.add(Field.create(true));

    Field listField = Field.create(list1);

    LinkedHashMap<String, Field> listMap = listField.getValueAsListMap();
    Assert.assertEquals(3, listMap.size());
    firstField = listMap.get("0");
    Assert.assertEquals("stringValue", firstField.getValue());
    secondField = listMap.get("1");
    Assert.assertEquals(40, secondField.getValue());
    Field thirdField = listMap.get("2");
    Assert.assertEquals(true, thirdField.getValue());



    //Test Map to ListMap
    Map<String, Field> map1 = new HashMap<>(3);
    map1.put("column1", Field.create("stringValue"));
    map1.put("column2", Field.create(40));
    map1.put("column3", Field.create(true));

    Field mapField = Field.create(map1);

    listMap = mapField.getValueAsListMap();
    Assert.assertEquals(3, listMap.size());
    firstField = listMap.get("column1");
    Assert.assertEquals("stringValue", firstField.getValue());
    secondField = listMap.get("column2");
    Assert.assertEquals(40, secondField.getValue());
    thirdField = listMap.get("column3");
    Assert.assertEquals(true, thirdField.getValue());

  }


}
