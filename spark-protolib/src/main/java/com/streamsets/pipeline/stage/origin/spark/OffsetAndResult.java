/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import java.util.List;

public class OffsetAndResult<T> {
  private final String offset;
  private final List<T> result;

  public OffsetAndResult(String offset, List<T> result) {
    this.offset = offset;
    this.result = result;
  }

  public String getOffset() {
    return offset;
  }

  public List<T> getResult() {
    return result;
  }
}
