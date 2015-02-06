/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.BaseError;

import java.text.ParseException;
import java.util.Date;

// we are making a Field for Date's to have 'immutable' values by cloning on create/get/clone
public class DateTypeSupport extends TypeSupport<Date> {

  @Override
  public Date convert(Object value) {
    if (value instanceof Date) {
      return (Date) value;
    }
    if (value instanceof String) {
      try {
        return Utils.parse((String) value);
      } catch (ParseException ex) {
        throw new IllegalArgumentException(Utils.format(BaseError.BASE_0007.getMessage(), value));
      }
    }
    if (value instanceof Long) {
      return new Date((long) value);
    }
    throw new IllegalArgumentException(Utils.format(BaseError.BASE_0008.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

  @Override
  public Object create(Object value) {
    return clone(value);
  }

  @Override
  public Object get(Object value) {
    return clone(value);
  }

  @Override
  public Object clone(Object value) {
    return ((Date) value).clone();
  }

}
