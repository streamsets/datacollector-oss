/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

public class BooleanField extends Field<Boolean> {

  public BooleanField(boolean value) {
    super(Type.BOOLEAN, value, true, null);
  }

  public BooleanField(String value) {
    super(Type.BOOLEAN, Boolean.parseBoolean(value), true, value);
  }
}
