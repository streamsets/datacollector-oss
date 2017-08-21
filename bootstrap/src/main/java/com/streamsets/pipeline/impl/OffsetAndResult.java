/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.impl;

import java.util.Collections;
import java.util.List;

public class OffsetAndResult<T> {
  private final Object offset;
  private final List<T> result;

  public OffsetAndResult(Object offset, List<T> result) {
    this.offset = offset;
    this.result = result == null ? Collections.<T>emptyList() : result;
  }

  public Object getOffset() {
    return offset;
  }

  public List<T> getResult() {
    return result;
  }

  @Override
  public String toString() {
    return "OffsetAndResult{" +
      "offset='" + offset + '\'' +
      ", result=" + (result == null ? "null" : result.size()) +
      '}';
  }
}
