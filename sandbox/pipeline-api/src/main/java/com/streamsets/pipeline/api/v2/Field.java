/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v2;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface Field {

  public enum Type {
    BOOLEAN(Boolean.class),
    BYTE(Byte.class),
    CHARACTER(Character.class),
    SHORT(Short.class),
    INTEGER(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    DECIMAL(BigDecimal.class),
    TIMESTAMP(Long.class),
    DATE(Date.class),
    DATETIME(Date.class),
    STRING(String.class),
    MAP(SimpleMap.class),
    ARRAY(List.class);

    private Class type;

    private Type(Class type) {
      this.type = type;
    }

    public Class getBaseClass() {
      return type;
    }
  }

  public Type getType();

  public Object getValue();

}
