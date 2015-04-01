/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.expression;

import com.streamsets.pipeline.api.ElFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ELSupport {

  //TODO: decide prefix. These functions seem very similar to uuid which is in DataUtilEL and has no prefix.
  @ElFunction(
    prefix = "",
    name = "emptyMap",
    description = "Creates an empty map")
  public static Map createEmptyMap() {
    return new HashMap<>();
  }

  @ElFunction(
    prefix = "",
    name = "emptyList",
    description = "Creates an empty list")
  public static List createEmptyList() {
    return new ArrayList<>();
  }
}
