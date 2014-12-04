/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.streamsets.pipeline.api.Field;

import java.util.ArrayList;
import java.util.List;

public class ListTypeSupport extends TypeSupport<List> {

  @Override
  public List convert(Object value) {
    if (value instanceof List) {
      return (List) value;
    }
    throw new IllegalArgumentException(Utils.format("Cannot convert {} '{}' to a List",
                                                    value.getClass().getSimpleName(), value));
  }

  @Override
  public Object convert(Object value, TypeSupport targetTypeSupport) {
    if (targetTypeSupport instanceof ListTypeSupport) {
      return value;
    } else {
      throw new IllegalArgumentException(Utils.format("Cannot convert List to other type, {}", targetTypeSupport));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object clone(Object value) {
    List List = null;
    if (value != null) {
      List = deepCopy((List<Field>)value);
    }
    return List;
  }

  private List<Field> deepCopy(List<Field> list) {
    List<Field> copy = new ArrayList<>(list.size());
    for (Field field : list) {
      Utils.checkNotNull(field, "List cannot have null elements");
      copy.add(field.clone());
    }
    return copy;
  }

}
