/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.impl;

import java.util.List;

public class OffsetAndResult<T> {
  private final Object offset;
  private final List<T> result;

  public OffsetAndResult(Object offset, List<T> result) {
    this.offset = offset;
    this.result = result;
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
