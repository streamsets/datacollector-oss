/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.common;

import java.nio.charset.Charset;

public class DataFormatConstants {

  public final static Charset UTF8 = Charset.forName("UTF-8");
  public static final int MAX_OVERRUN_LIMIT = Integer.parseInt(
    System.getProperty("DataFactoryBuilder.OverRunLimit", "1000000"));
}
