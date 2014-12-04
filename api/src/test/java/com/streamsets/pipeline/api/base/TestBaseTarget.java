/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import org.junit.Test;

public class TestBaseTarget {

  @Test
  public void testConstructor() {
    new BaseTarget() {
      @Override
      public void write(Batch batch) throws StageException {
      }
    };
  }


}
