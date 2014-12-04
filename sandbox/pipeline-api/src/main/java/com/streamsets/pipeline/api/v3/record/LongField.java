/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

public class LongField extends Field<Long> {

  public LongField(long value) {
    super(Type.LONG, value, true, null);
  }

  public LongField(String value) {
    super(Type.LONG, Long.parseLong(value), true, value);
  }

}
