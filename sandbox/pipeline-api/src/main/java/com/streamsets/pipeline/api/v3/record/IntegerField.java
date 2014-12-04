/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

public class IntegerField extends Field<Integer> {

  public IntegerField(int value) {
    super(Type.INTEGER, value, true, null);
  }

  public IntegerField(String value) {
    super(Type.INTEGER, Integer.parseInt(value), true, value);
  }

}
