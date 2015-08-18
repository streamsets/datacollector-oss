/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.base.Errors;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class ListTypeSupport extends TypeSupport<List> {

  @Override
  public List convert(Object value) {
    if (value instanceof List) {
      return (List) value;
    }
    throw new IllegalArgumentException(Utils.format(Errors.API_12.getMessage(),
                                                    value.getClass().getSimpleName(), value));
  }

  @Override
  public Object convert(Object value, TypeSupport targetTypeSupport) {
    if (targetTypeSupport instanceof ListTypeSupport) {
      return value;
    } else if(targetTypeSupport instanceof ListMapTypeSupport) {
      List list = (List) value;
      LinkedHashMap<String, Field> listMap = new LinkedHashMap<>(list.size());
      for (int i = 0; i < list.size(); i++) {
        listMap.put(i + "", (Field)list.get(i));
      }
      return listMap;
    } else {
      throw new IllegalArgumentException(Utils.format(Errors.API_13.getMessage(), targetTypeSupport));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object clone(Object value) {
    List list = null;
    if (value != null) {
      list = deepCopy((List<Field>)value);
    }
    return list;
  }

  private List<Field> deepCopy(List<Field> list) {
    List<Field> copy = new ArrayList<>(list.size());
    for (int i = 0; i < list.size(); i++) {
      Field field = list.get(i);
      Utils.checkNotNull(field, Utils.formatL("List has null element at '{}' pos", i));
      copy.add(field.clone());
    }
    return copy;
  }

  @Override
  public Object create(Object value) {
    return clone(value);
  }

}
