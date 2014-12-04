/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

public class TimestampField extends Field<Long> {

  public TimestampField(long value) {
    super(Type.TIMESTAMP, value, true, null);
  }

  public TimestampField(String value) {
    super(Type.TIMESTAMP, Long.parseLong(value), true, value);
  }

}
