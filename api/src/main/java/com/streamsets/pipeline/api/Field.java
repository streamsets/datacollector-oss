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

import java.math.BigDecimal;
import java.util.Date;

// A field is immutable, doing special handling for Date, cloning it on value()
public class Field {

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
    BYTE_ARRAY(new ByteArrayTypeSupport());

    private final TypeSupport<?> support;

    private Type(TypeSupport<?> support) {
      this.support = support;
    }

    private Object convert(Object value) {
      return (value != null) ? support.convert(value) : null;
    }

    @SuppressWarnings("unchecked")
    private <T> T snapshot(T value) {
      return (value != null) ? (T) support.snapshot(value) : null;
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

  public static Field createDate(Date v) {
    return new Field(Type.DATE, v);
  }

  public static Field createDatetime(Date v) {
    return new Field(Type.DATETIME, v);
  }

  public static <T> Field create(Type type, T value) {
    return new Field(ApiUtils.checkNotNull(type, "type"), type.convert(value));
  }

  public static <T> Field create(Field field, T value) {
    return create(ApiUtils.checkNotNull(field, "field").getType(), value);
  }

  private Type type;
  private Object value;

  private Field(Type type, Object value) {
    this.type = ApiUtils.checkNotNull(type, "type");
    this.value = type.snapshot(value);
  }

  public Type getType() {
    return type;
  }

  public Object getValue() {
    return type.snapshot(value);
  }

  public String toString() {
    return (value != null) ? value.toString() : null;
  }

}
