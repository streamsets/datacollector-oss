/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

public class FloatField extends Field<Float> {

  public FloatField(float value) {
    super(Type.FLOAT, value, true, null);
  }

  public FloatField(String value) {
    super(Type.FLOAT, Float.parseFloat(value), true, value);
  }

}
