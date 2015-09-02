/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.base.Errors;

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
        throw new IllegalArgumentException(Utils.format(Errors.API_06.getMessage(), value));
      }
    }
    if (value instanceof Long) {
      return new Date((long) value);
    }
    throw new IllegalArgumentException(Utils.format(Errors.API_07.getMessage(),
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
