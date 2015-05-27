/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElConstant;

public class DataUnitsEL {

  @ElConstant(name = "KB", description = "Kilobytes")
  public static final int KB = 1000;

  @ElConstant(name = "MB", description = "Megabytes")
  public static final int MB = 1000 * KB;

  @ElConstant(name = "GB", description = "Gigabytes")
  public static final int GB = 1000 * MB;

}
