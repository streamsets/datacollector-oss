/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElConstant;

public class ApacheLogEL {

  @ElConstant(name = "IP Address", description = "%a")
  public static final int IP_ADDRESS = 0x0001;


}
