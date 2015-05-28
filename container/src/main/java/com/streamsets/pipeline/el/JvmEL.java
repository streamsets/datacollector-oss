/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.ElFunction;

public class JvmEL {

  @ElFunction(prefix = "jvm", name = "maxMemory",
      description = "JVM Maximum Heap size, in bytes"
  )
  public static long jvmMaxMemory() {
    return  Runtime.getRuntime().maxMemory();
  }

}
