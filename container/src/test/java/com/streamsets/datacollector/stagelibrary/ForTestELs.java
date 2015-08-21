/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.stagelibrary;

import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;

public class ForTestELs {

  @ElConstant(name = "FOOBAR", description="")
  public static final String CONSTANT = "";

  @ElFunction(prefix = "foo", name = "bar", description = "")
  public static String function() {
    return null;
  }

}
