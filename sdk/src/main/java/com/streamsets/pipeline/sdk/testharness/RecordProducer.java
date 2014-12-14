/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.testharness;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.Date;
import java.util.Random;

public class RecordProducer {
  private static final Random RANDOM = new Random();
  private static long counter;

  private char[] values = {
    'a','b','c','d','e','f','g','h','i','j','k','l','m',
    'n','o','p','q','r','s','t','u','v','w','x','y','z'};

  public RecordProducer() {
  }

  public Record create() {
    return new RecordImpl("RecordProducer", "RecordProducer::" + counter++, null, null);
  }

  private Field createRandomField(Field.Type type) {
    switch(type) {
      case BOOLEAN:
        return Field.create(RANDOM.nextBoolean());
      case BYTE:
        return Field.create((byte) RANDOM.nextInt());
      case BYTE_ARRAY:
        byte[] bytes = new byte[128];
        RANDOM.nextBytes(bytes);
        return Field.create(bytes);
      case CHAR:
        return Field.create(values[RANDOM.nextInt(values.length)]);
      case DATE:
        return Field.createDate(new Date());
      case DATETIME:
        return Field.createDatetime(new Date());
      case DECIMAL:
      case DOUBLE:
        return Field.create(RANDOM.nextDouble());
      case FLOAT:
        return Field.create(RANDOM.nextFloat());
      case INTEGER:
        return Field.create(RANDOM.nextInt());
      case LONG:
        return Field.create(RANDOM.nextLong());
      case SHORT:
        return Field.create((short) RANDOM.nextInt());
      case STRING:
        return Field.create(generateRandomString(50));
      default:
        throw new IllegalStateException("Unexpected");
    }
  }

  private String generateRandomString(int length) {
    StringBuilder sb = new StringBuilder();
    for (int i=0;i<length;i++) {
      int idx= RANDOM.nextInt(values.length);
      sb.append(values[idx]);
    }
    return sb.toString();
  }

}
