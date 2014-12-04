/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

public class ShortField extends Field<Short> {

  public ShortField(short value) {
    super(Type.SHORT, value, true, null);
  }

  public ShortField(String value) {
    super(Type.SHORT, Short.parseShort(value), true, value);
  }

}
