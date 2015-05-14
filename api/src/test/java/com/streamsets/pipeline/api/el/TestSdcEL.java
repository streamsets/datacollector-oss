/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.el;

import com.streamsets.pipeline.api.impl.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;

public class TestSdcEL {

  @Test
  public void testId() {
    Utils.setSdcIdCallable(new Callable<String>() {
      @Override
      public String call() throws Exception {
        return "x";
      }
    });
    Assert.assertEquals("x", SdcEL.getId());
  }

}
