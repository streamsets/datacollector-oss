/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

public class DoubleField extends Field<Double> {

  public DoubleField(double value) {
    super(Type.DOUBLE, value, true, null);
  }

  public DoubleField(String value) {
    super(Type.DOUBLE, Double.parseDouble(value), true, value);
  }

}
