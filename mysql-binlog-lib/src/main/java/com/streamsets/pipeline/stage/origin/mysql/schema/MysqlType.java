/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.mysql.schema;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Field.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mysql types supported by mysql connector.
 */
public enum MysqlType {
  DECIMAL("decimal") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.DECIMAL, value);
    }
  },
  TINY_INT("tinyint") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.INTEGER, value);
    }
  },
  SMALL_INT("smallint") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.INTEGER, value);
    }
  },
  MEDIUM_INT("mediumint") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.INTEGER, value);
    }
  },
  FLOAT("float") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.FLOAT, value);
    }
  },
  DOUBLE("double") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.DOUBLE, value);
    }
  },
  TIMESTAMP("timestamp") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.DATETIME, value);
    }
  },
  BIGINT("bigint") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.LONG, value);
    }
  },
  INT("int") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.INTEGER, value);
    }
  },
  DATE("date") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.DATE, value);
    }
  },
  TIME("time") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.TIME, value);
    }
  },
  DATETIME("datetime") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.DATETIME, value);
    }
  },
  YEAR("year") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.INTEGER, value);
    }
  },
  VARCHAR("varchar") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.STRING, value);
    }
  },
  ENUM("enum") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.INTEGER, value);
    }
  },
  SET("set") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.LONG, value);
    }
  },
  TINY_BLOB("tinyblob") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.BYTE_ARRAY, value);
    }
  },
  MEDIUM_BLOB("mediumblob") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.BYTE_ARRAY, value);
    }
  },
  LONG_BLOB("longblob") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.BYTE_ARRAY, value);
    }
  },
  BLOB("blob") {
    @Override
    public Field toField(Object value) {
      return Field.create(Type.BYTE_ARRAY, value);
    }
  },
  TEXT("text") {
    @Override
    public Field toField(Object value) {
      return stringField(value);
    }
  },
  TINY_TEXT("tinytext") {
    @Override
    public Field toField(Object value) {
      return stringField(value);
    }
  },
  MEDIUM_TEXT("mediumtext") {
    @Override
    public Field toField(Object value) {
      return stringField(value);
    }
  },
  LONG_TEXT("longtext") {
    @Override
    public Field toField(Object value) {
      return stringField(value);
    }
  },
  UNSUPPORTED("unsupported") {
    @Override
    public Field toField(Object value) {
      return stringField(value);
    }
  };

  private static final Logger LOG = LoggerFactory.getLogger(MysqlType.class);

  private static Field stringField(Object value) {
    if (value == null) {
      return Field.create(Type.STRING, null);
    } else {
      if (value instanceof byte[]) {
        return Field.create(Type.STRING, new String((byte[]) value));
      } else {
        return Field.create(Type.STRING, value.toString());
      }
    }
  }

  private final String name;

  public abstract Field toField(Object value);

  MysqlType(String name) {
    this.name = name;
  }

  public static MysqlType of(String name) {
    // at least now we are not interested in precision - cut it off
    String typeName = name;
    int i = typeName.indexOf('(');
    if (i > -1) {
      typeName = typeName.substring(0, i);
    }

    for (MysqlType t : MysqlType.values()) {
      if (t.name.toLowerCase().equals(typeName.toLowerCase())) {
        return t;
      }
    }
    LOG.warn("Encountered unsupported mysql type {}", name);
    return UNSUPPORTED;
  }
}
