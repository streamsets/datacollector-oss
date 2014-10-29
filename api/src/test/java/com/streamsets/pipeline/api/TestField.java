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

import com.streamsets.pipeline.api.Field.Type;
import com.streamsets.pipeline.container.BooleanTypeSupport;
import com.streamsets.pipeline.container.ByteArrayTypeSupport;
import com.streamsets.pipeline.container.ByteTypeSupport;
import com.streamsets.pipeline.container.CharTypeSupport;
import com.streamsets.pipeline.container.DateTypeSupport;
import com.streamsets.pipeline.container.DecimalTypeSupport;
import com.streamsets.pipeline.container.DoubleTypeSupport;
import com.streamsets.pipeline.container.FloatTypeSupport;
import com.streamsets.pipeline.container.IntegerTypeSupport;
import com.streamsets.pipeline.container.LongTypeSupport;
import com.streamsets.pipeline.container.ShortTypeSupport;
import com.streamsets.pipeline.container.StringTypeSupport;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;

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

    f = Field.create((byte)1);
    Assert.assertEquals(Type.BYTE, f.getType());
    Assert.assertEquals((byte)1, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create((short)2);
    Assert.assertEquals(Type.SHORT, f.getType());
    Assert.assertEquals((short)2, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create(3);
    Assert.assertEquals(Type.INTEGER, f.getType());
    Assert.assertEquals(3, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create((long)4);
    Assert.assertEquals(Type.LONG, f.getType());
    Assert.assertEquals((long)4, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create((float)5);
    Assert.assertEquals(Type.FLOAT, f.getType());
    Assert.assertEquals((float)5, f.getValue());
    Assert.assertNotNull(f.toString());

    f = Field.create((double)6);
    Assert.assertEquals(Type.DOUBLE, f.getType());
    Assert.assertEquals((double)6, f.getValue());
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
    Field.create((Type)null, null);
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

}
