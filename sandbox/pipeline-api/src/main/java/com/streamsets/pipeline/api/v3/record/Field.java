/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
