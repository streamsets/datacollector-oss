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

public abstract class TypeSupport<T> {

  public abstract T convert(Object value);

  public Object convert(Object value, TypeSupport targetTypeSupport) {
    return targetTypeSupport.convert(value);
  }

  public Object create(Object value) {
    return value;
  }

  public Object get(Object value) {
    return value;
  }

  // default implementation assumes value is immutable, no need to clone
  public Object clone(Object value) {
    return value;
  }

  public boolean equals(Object value1, Object value2) {
    return (value1 == value2) || (value1 != null && value2 != null && value1.equals(value2));
  }

}
