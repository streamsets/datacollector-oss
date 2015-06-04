/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.ElFunction;

public class JvmEL {

  @ElFunction(prefix = "jvm", name = "maxMemoryMB",
      description = "JVM Maximum Heap size, in MB"
  )
  public static long jvmMaxMemoryMB() {
    return  Runtime.getRuntime().maxMemory() / 1000 / 1000;
  }

}
