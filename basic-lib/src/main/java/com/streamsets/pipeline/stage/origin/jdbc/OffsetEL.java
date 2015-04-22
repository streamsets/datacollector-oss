/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.ElConstant;

public class OffsetEL {
  @ElConstant(name = "OFFSET", description = "")
  public static final String OFFSET = "${offset}";
}
