/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.UUID;

/**
 * EL functions which are useful when transforming data but not useful
 * when evaluating conditions for alerts.
 */
public class DataUtilEL {

  @ElFunction(prefix = "", name = "uuid", description = "generates uuid")
  public static String UUIDFunc() {
    return UUID.randomUUID().toString();
  }
}
