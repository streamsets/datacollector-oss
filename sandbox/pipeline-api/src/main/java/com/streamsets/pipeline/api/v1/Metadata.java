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
