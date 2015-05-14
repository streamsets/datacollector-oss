/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.impl.Utils;

public class SdcEL {

  private static final String PREFIX = "sdc";

  @ElFunction(prefix = PREFIX, name = "id", description = "Unique ID for the SDC")
  public static String getId() {
    return Utils.getSdcId();
  }

}
