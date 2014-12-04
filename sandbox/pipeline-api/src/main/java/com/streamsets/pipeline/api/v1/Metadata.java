/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v1;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Metadata {

  public enum Type {
    BOOLEAN(Boolean.class),
    BYTE(Byte.class),
    CHAR(Character.class),
    SHORT(Short.class),
    INTEGER(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    NUMBER(BigDecimal.class),
    STRING(String.class),
    MAP(Map.class),
    ARRAY(List.class);

    private Class type;

    private Type(Class type) {
      this.type = type;
    }

    public Class getType() {
      return type;
    }

  }

  private final String name;
  private final Type type;
  private final String format;
  private final Map<String, Metadata> mapMetadata;
  private final List<Metadata> arrayMetadata;

  private Metadata(String name, Type type, String format, Map<String, Metadata> mapMetadata,
      List<Metadata> arrayMetadata) {
    this.name = name;
    this.type = type;
    this.format = format;
    this.mapMetadata = mapMetadata;
    this.arrayMetadata = arrayMetadata;
  }

  public String getName() {
    return name;
  }
  public Type getType() {
    return type;
  }

  //printf syntax
  public String getFormat() {
    return format;
  }

  // only if type==MAP
  public Map<String, Metadata> getMapMetadata() {
    return mapMetadata;
  }

  // only if type==ARRAY
  public List<Metadata> getArrayMetadata() {
    return arrayMetadata;
  }

  @SuppressWarnings("unchecked")
  public static class Builder {
    private String name;
    private Type type;
    private String format;
    private Map<String, Metadata> mapMetadata;
    private List<Metadata> arrayMetadata;

    public Builder() {
      format = "%s";
      mapMetadata = Collections.EMPTY_MAP;
      arrayMetadata = Collections.EMPTY_LIST;
    }

    public void setName(String fieldName) {
      name = fieldName;
    }

    public void setType(Type type) {
      this.type = type;
    }

    public void setFormat(String format) {
      this.format = format;
    }

    public void setMapMetadata(Map<String, Metadata> metadata) {
      type = Type.MAP;
      mapMetadata = new HashMap<String, Metadata>(metadata);
    }

    public void setArrayMetadata(List<Metadata> metadata) {
      type = Type.ARRAY;
      arrayMetadata = new ArrayList<Metadata>(metadata);
    }

    public Metadata build() {
      return new Metadata(name, type, format, mapMetadata, arrayMetadata);
    }

  }
}
