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

import com.streamsets.pipeline.container.ListTypeSupport;
import com.streamsets.pipeline.container.MapTypeSupport;
import com.streamsets.pipeline.container.Utils;
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
import com.streamsets.pipeline.container.TypeSupport;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

// A field is immutable
public class Field implements Cloneable {

  public enum Type {
    BOOLEAN(new BooleanTypeSupport()),
    CHAR(new CharTypeSupport()),
    BYTE(new ByteTypeSupport()),
    SHORT(new ShortTypeSupport()),
    INTEGER(new IntegerTypeSupport()),
    LONG(new LongTypeSupport()),
    FLOAT(new FloatTypeSupport()),
    DOUBLE(new DoubleTypeSupport()),
    DATE(new DateTypeSupport()),
    DATETIME(new DateTypeSupport()),
    DECIMAL(new DecimalTypeSupport()),
    STRING(new StringTypeSupport()),
    BYTE_ARRAY(new ByteArrayTypeSupport()),
    MAP(new MapTypeSupport()),
    LIST(new ListTypeSupport());

    final TypeSupport<?> supporter;

    private Type(TypeSupport<?> supporter) {
      this.supporter = supporter;
    }

    private Object convert(Object value) {
      return (value != null) ? supporter.convert(value) : null;
    }

    private Object convert(Object value, Type targetType) {
      return (value != null) ? supporter.convert(value, targetType.supporter) : null;
    }

    @SuppressWarnings("unchecked")
    private <T> T constructorCopy(T value) {
      return (value != null) ? (T) supporter.constructorCopy(value) : null;
    }

    @SuppressWarnings("unchecked")
    private <T> T getReference(T value) {
      return (value != null) ? (T) supporter.getReference(value) : null;
    }

    private String toString(Object value) {
      return Utils.format("Field[{}:{}]", this, value);
    }

  }

  public static Field create(boolean v) {
    return new Field(Type.BOOLEAN, v);
  }

  public static Field create(char v) {
    return new Field(Type.CHAR, v);
  }

  public static Field create(byte v) {
    return new Field(Type.BYTE, v);
  }

  public static Field create(short v) {
    return new Field(Type.SHORT, v);
  }

  public static Field create(int v) {
    return new Field(Type.INTEGER, v);
  }

  public static Field create(long v) {
    return new Field(Type.LONG, v);
  }

  public static Field create(float v) {
    return new Field(Type.FLOAT, v);
  }

  public static Field create(double v) {
    return new Field(Type.DOUBLE, v);
  }

  public static Field create(BigDecimal v) {
    return new Field(Type.DECIMAL, v);
  }

  public static Field create(String v) {
    return new Field(Type.STRING, v);
  }

  public static Field create(byte[] v) {
    return new Field(Type.BYTE_ARRAY, v);
  }

  // copy
  public static Field createDate(Date v) {
    return new Field(Type.DATE, v);
  }

  // copy
  public static Field createDatetime(Date v) {
    return new Field(Type.DATETIME, v);
  }

  // deep copy
  public static Field create(Map<String, Field> v) {
    return new Field(Type.MAP, v);
  }

  // deep copy
  public static Field create(List<Field> v) {
    return new Field(Type.LIST, v);
  }

  // deep copy
  public static <T> Field create(Type type, T value) {
    return new Field(Utils.checkNotNull(type, "type"), type.convert(value));
  }

  // deep copy
  public static <T> Field create(Field field, T value) {
    return create(Utils.checkNotNull(field, "field").getType(), value);
  }


  private Type type;
  private Object value;

  private Field(Type type, Object value) {
    this.type = type;
    this.value = type.constructorCopy(value);
  }

  public Type getType() {
    return type;
  }

  // by ref
  public Object getValue() {
    return type.getReference(value);
  }

  public boolean getValueAsBoolean() {
    return (boolean) type.convert(getValue(), Type.BOOLEAN);
  }

  public char getValueAsChar() {
    return (char) type.convert(getValue(), Type.CHAR);
  }

  public byte getValueAsByte() {
    return (byte) type.convert(getValue(), Type.BYTE);
  }

  public short getValueAsShort() {
    return (short) type.convert(getValue(), Type.SHORT);
  }

  public int getValueAsInteger() {
    return (int) type.convert(getValue(), Type.INTEGER);
  }

  public long getValueAsLong() {
    return (long) type.convert(getValue(), Type.LONG);
  }

  public float getValueAsFloat() {
    return (float) type.convert(getValue(), Type.FLOAT);
  }

  public double getValueAsDouble() {
    return (double) type.convert(getValue(), Type.DOUBLE);
  }

  // copy, date is handled as immutable
  public Date getValueAsDate() {
    return (Date) type.convert(getValue(), Type.DATE);
  }

  // copy, date is handled as immutable
  public Date getValueAsDatetime() {
    return (Date) type.convert(getValue(), Type.DATE);
  }

  public BigDecimal getValueAsDecimal() {
    return (BigDecimal) type.convert(getValue(), Type.DECIMAL);
  }

  public String getValueAsString() {
    return (String) type.convert(getValue(), Type.STRING);
  }

  // copy, byte[] is handled as immutable
  public byte[] getValueAsByteArray() {
    return (byte[]) type.convert(getValue(), Type.BYTE_ARRAY);
  }

  // by ref
  @SuppressWarnings("unchecked")
  public Map<String, Field> getValueAsMap() {
    return (Map<String, Field>) type.convert(getValue(), Type.MAP);
  }

  // by ref
  @SuppressWarnings("unchecked")
  public List<Field> getValueAsList() {
    return (List<Field>) type.convert(getValue(), Type.LIST);
  }

  @Override
  public String toString() {
    return type.toString(value);
  }

  @Override
  public int hashCode() {
    return (value != null) ? value.hashCode() : 0;
  }

  @Override
  public boolean equals(Object obj) {
    boolean eq = false;
    if (obj != null) {
      if (obj instanceof Field) {
        Field other = (Field) obj;
        if (type == other.type) {
          eq = (value == other.value) || (value != null && value.equals(other.value));
        }
      }
    }
    return eq;
  }

  @Override
  public Field clone() {
    switch (type) {
      case MAP:
      case LIST:
        try {
          Field clone = (Field) super.clone();
          clone.type = type;
          clone.value = type.constructorCopy(value);
          return clone;
        } catch (CloneNotSupportedException ex) {
         //
        }
      default:
        return this;
    }
  }

}
