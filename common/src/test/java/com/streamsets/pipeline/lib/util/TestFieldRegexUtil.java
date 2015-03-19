/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import org.junit.Assert;
import org.junit.Test;

public class TestFieldRegexUtil {

  @Test
  public void testFieldRegexUtil(){
    Assert.assertTrue(FieldRegexUtil.hasWildCards("USA/*/CA"));
    Assert.assertTrue(FieldRegexUtil.hasWildCards("USA[*]/CA"));
    Assert.assertFalse(FieldRegexUtil.hasWildCards("USA[1]/CA"));
    Assert.assertFalse(FieldRegexUtil.hasWildCards("USA/CA"));
  }


}
