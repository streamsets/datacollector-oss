/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.ElFunction;

import java.util.UUID;

public class MiscEl {

  @ElFunction(prefix = "", name = "uuid", description = "generates uuid")
  public static String UUIDFunc() {
    return UUID.randomUUID().toString();
  }
}
