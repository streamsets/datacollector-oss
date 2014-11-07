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
package com.streamsets.pipeline.sdk.testharness;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class RecordProducer {

  public enum Type {
    BOOLEAN,
    CHAR,
    BYTE,
    SHORT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE,
    DATETIME,
    DECIMAL,
    STRING,
    BYTE_ARRAY
  }

  private Map<String, Type> fieldMap = null;
  private Random random;
  private char[] values = {
    'a','b','c','d','e','f','g','h','i','j','k','l','m',
    'n','o','p','q','r','s','t','u','v','w','x','y','z'};

  public RecordProducer() {
    random = new Random();
    this.fieldMap = new HashMap<String, Type>();
  }

  public RecordProducer addFiled(String name, Type type) {
    fieldMap.put(name, type);
    return this;
  }

  public Record produce() {
    Record r = new RecordImpl("recordProducer",
      "RecordProducer", null, null);
    for(Map.Entry<String, Type> e : fieldMap.entrySet()) {
      r.setField(e.getKey(), createField(e.getValue()));
    }
    return r;
  }

  private Field createField(Type type) {

    switch(type) {
      case BOOLEAN:
        return Field.create(random.nextBoolean());
      case BYTE:
        return Field.create((byte)random.nextInt());
      case BYTE_ARRAY:
        byte[] bytes = new byte[128];
        random.nextBytes(bytes);
        return Field.create(bytes);
      case CHAR:
        return Field.create(values[random.nextInt(values.length)]);
      case DATE:
        return Field.createDate(new Date());
      case DATETIME:
        return Field.createDatetime(new Date());
      case DECIMAL:
      case DOUBLE:
        return Field.create(random.nextDouble());
      case FLOAT:
        return Field.create(random.nextFloat());
      case INTEGER:
        return Field.create(random.nextInt());
      case LONG:
        return Field.create(random.nextLong());
      case SHORT:
        return Field.create((short)random.nextInt());
      case STRING:
        return Field.create(generateRandomString(50));
      default:
        throw new IllegalStateException("Unexpected");
    }
  }

  private String generateRandomString(int length) {
    StringBuilder sb = new StringBuilder();
    for (int i=0;i<length;i++) {
      int idx=random.nextInt(values.length);
      sb.append(values[idx]);
    }
    return sb.toString();
  }

}
