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
import java.util.List;
import java.util.Map;

public interface FieldMetadata {

  public String getName();

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

  public Type getType();

  public String getFormat(); //printf syntax

  public Map<String, FieldMetadata> getMapMetadata(); // only if type==MAP

  public List<FieldMetadata> getArrayMetadata(); // only if type==ARRAY

}
