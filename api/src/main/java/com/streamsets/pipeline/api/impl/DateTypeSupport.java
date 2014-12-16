/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

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
        throw new IllegalArgumentException(Utils.format("Cannot parse '{}' to a Date, format must be ISO8601 UTC" +
                                                        "(yyyy-MM-dd'T'HH:mm'Z')", value));
      }
    }
    throw new IllegalArgumentException(Utils.format("Cannot convert {} '{}' to a Date",
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
