/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.Errors;

import java.util.List;
import java.util.Map;

public class StringTypeSupport extends TypeSupport<String> {

  @Override
  public String convert(Object value) {
    if(value instanceof Map || value instanceof List || value instanceof byte[]) {
      throw new IllegalArgumentException(Utils.format(Errors.API_18.getMessage()));
    }
    return value.toString();
  }

}
