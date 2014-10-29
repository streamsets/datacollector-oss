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
package com.streamsets.pipeline.container;

import java.text.ParseException;
import java.util.Date;

public class DateTypeSupport extends TypeSupport<Date> {

  @Override
  public Date convert(Object value) {
    if (value instanceof Date) {
      return (Date) value;
    }
    if (value instanceof String) {
      try {
        return ApiUtils.parse((String) value);
      } catch (ParseException ex) {
        throw new IllegalArgumentException(ApiUtils.format("Cannot parse '{}' to a Date, format must be ISO8601 UTC" +
                                                           "(yyyy-MM-dd'T'HH:mm'Z')", value));
      }
    }
    throw new IllegalArgumentException(ApiUtils.format("Cannot convert {} '{}' to a Date",
                                                       value.getClass().getSimpleName(), value));
  }

  @Override
  public Object snapshot(Object value) {
    return ((Date) value).clone();
  }

}
