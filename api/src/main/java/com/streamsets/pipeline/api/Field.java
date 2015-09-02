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

import com.streamsets.pipeline.api.impl.BooleanTypeSupport;
import com.streamsets.pipeline.api.impl.ByteArrayTypeSupport;
import com.streamsets.pipeline.api.impl.ByteTypeSupport;
import com.streamsets.pipeline.api.impl.CharTypeSupport;
import com.streamsets.pipeline.api.impl.CreateByRef;
import com.streamsets.pipeline.api.impl.DateTypeSupport;
import com.streamsets.pipeline.api.impl.DecimalTypeSupport;
import com.streamsets.pipeline.api.impl.DoubleTypeSupport;
import com.streamsets.pipeline.api.impl.FloatTypeSupport;
import com.streamsets.pipeline.api.impl.IntegerTypeSupport;
import com.streamsets.pipeline.api.impl.ListMapTypeSupport;
import com.streamsets.pipeline.api.impl.ListTypeSupport;
import com.streamsets.pipeline.api.impl.LongTypeSupport;
import com.streamsets.pipeline.api.impl.MapTypeSupport;
import com.streamsets.pipeline.api.impl.ShortTypeSupport;
import com.streamsets.pipeline.api.impl.StringTypeSupport;
import com.streamsets.pipeline.api.impl.TypeSupport;
import com.streamsets.pipeline.api.impl.Utils;

import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A <code>Field</code> is a type/value pair, where the type o the value matches the <code>Field</code> type.
 * <code>Field</code> values can be automatically converted to compatible Java primitive types and classes.
 * <code>Field</code> values can be <code>null</code>.
 * <p></p>
 * The {@link Type} enumeration defines the supported types for <code>Field</code> values.
 * <p></p>
 * Except for the Collection based types, <code>Field</code> values are immutable. Collection <code>Field</code> values
 * can be modified using the corresponding Collection manipulation methods.
 * <p></p>
 * <b>NOTE:</b> Java <code>Date</code> and <code>byte[]</code> are not immutable. <code>Field</code> makes immutable
 * by performing a copy on <code>create()</code> and on <code>get()</code>. This means that if a <code>Date</code> or
 * <code>byte[]</code> instance obtained from a <code>Field</code> is modified, the actual value stored in the
 * <code>Field</code> is not modified.
 * <p></p>
 * The {@link #hashCode}, {@link #equals} and {@link #clone} methods work in deep operation mode on the
 * <code>Field</code>.
 */
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
    LIST(new ListTypeSupport()),
    LIST_MAP(new ListMapTypeSupport());

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

    private boolean equals(Object value1, Object value2) {
      return supporter.equals(value1, value2);
    }

    @SuppressWarnings("unchecked")
    private <T> T constructorCopy(T value) {
      return (value != null) ? (T) supporter.create(value) : null;
    }

    @SuppressWarnings("unchecked")
    private <T> T getReference(T value) {
      return (value != null) ? (T) supporter.get(value) : null;
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
  public static Field createListMap(LinkedHashMap<String, Field> v) {
    return new Field(Type.LIST_MAP, v);
  }

  // deep copy for MAP and LIST type
  public static <T> Field create(Type type, T value) {
    return new Field(Utils.checkNotNull(type, "type"), type.convert(value));
  }

  // deep copy for MAP and LIST type
  public static <T> Field create(Field field, T value) {
    return create(Utils.checkNotNull(field, "field").getType(), value);
  }

  private Type type;
  private Object value;

  // need default constructor for deserialization purposes (Kryo)
  private Field() {
  }

  private Field(Type type, Object value) {
    this.type = type;
    this.value = (CreateByRef.isByRef()) ? value : type.constructorCopy(value);
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

  // by ref, changes to the returned Map will change the Map stored in the <code>Field</code
  @SuppressWarnings("unchecked")
  public Map<String, Field> getValueAsMap() {
    return (Map<String, Field>) type.convert(getValue(), Type.MAP);
  }

  // by ref, changes to the returned List will change the Map stored in the <code>Field</code
  // by ref
  @SuppressWarnings("unchecked")
  public List<Field> getValueAsList() {
    return (List<Field>) type.convert(getValue(), Type.LIST);
  }

  @SuppressWarnings("unchecked")
  public LinkedHashMap<String, Field> getValueAsListMap() {
    return (LinkedHashMap<String, Field>) type.convert(getValue(), Type.LIST_MAP);
  }

  @Override
  public String toString() {
    return type.toString(value);
  }

  // deep hashcode(), value based
  @Override
  public int hashCode() {
    return (value != null) ? value.hashCode() : 0;
  }

  // deep equals(), value based
  @Override
  public boolean equals(Object obj) {
    boolean eq = false;
    if (obj != null) {
      if (obj instanceof Field) {
        Field other = (Field) obj;
        if (type == other.type) {
          eq = (value == other.value) || type.equals(value, other.value);
        }
      }
    }
    return eq;
  }

  /**
   * Clones the <code>Field</code>.
   * <p></p>
   * For <code>Field</code> instances  of non-Collection based types it returns the same instance as the are immutable
   * (this deviates from the expected behavior documented in the  <code>Object.clone()</code>.
   * <p></p>
   * * For <code>Field</code> instances  of Collection based types it returns the deep copy of the <code>Field</code>
   * instance.
   *
   * @return a clone the <code>Filed</code> instance.
   */
  @Override
  public Field clone() {
    return (type != Type.MAP && type != Type.LIST && type != Type.LIST_MAP) ? this : new Field(type, value);
  }

  public void set(Type type, Object value) {
    this.type = type;
    this.value = type.constructorCopy(value);
  }

}
