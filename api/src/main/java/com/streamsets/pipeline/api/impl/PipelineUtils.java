/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

public final class PipelineUtils {

  PipelineUtils() {
  }

  public static String escapedPipelineName(String pipelineName) {
    return pipelineName.replaceAll(" ", ".");
  }
}
