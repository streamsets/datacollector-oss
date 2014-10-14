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
package com.streamsets.pipeline.api.v3.record;

import java.util.Locale;

public class Field<T> {

  public enum Type {
    BOOLEAN, BYTE, CHARACTER, SHORT, INTEGER, LONG, FLOAT, DOUBLE, NUMBER, STRING, DATE, DATETIME, TIMESTAMP, BINARY
  }

  private Type type;
  private T value;
  private boolean valid;
  private Object raw;
  
  Field(Type type, T value, boolean valid, Object raw) {
    this.type = type;
    this.value = value;
    this.valid = valid;
    this.raw = raw;
  }

  public Type getType() {
    return type;
  }

  public T getValue() {
    return value;
  }

  public boolean isValid() {
    return valid;
  }
  
  public String getRawAsString() {
    return (raw != null) ? raw.toString() : null;
  }

  protected String valueToString(Locale locale) {
    return value.toString(); //todo localize it
  }

  public String toString(Locale locale) {
    String str;
    if (isValid()) {
      if (value != null) {
        str = valueToString(locale);
      } else {
        str = "null";
      }
    } else {
      str = "Invalid"; //todo localizationbundle
    }
    return str;
  }

}
