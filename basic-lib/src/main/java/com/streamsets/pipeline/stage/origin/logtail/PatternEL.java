/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.ElConstant;

public class PatternEL {
  private static final String TOKEN_NAME = "PATTERN";

  public static final String TOKEN = "${" + TOKEN_NAME + "}";

  @ElConstant(name = TOKEN_NAME, description = "")
  public static final String PATTERN = TOKEN;

}
