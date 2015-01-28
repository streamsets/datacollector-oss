/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.BaseError;

public class CharTypeSupport extends TypeSupport<Character> {

  @Override
  public Character convert(Object value) {
    if (value instanceof Character) {
      return (Character) value;
    }
    if (value instanceof String) {
      String s = (String) value;
      if (s.length() > 0) {
        return s.charAt(0);
      }
    }
    throw new IllegalArgumentException(Utils.format(BaseError.BASE_0006.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

}
