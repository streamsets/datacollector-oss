/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;

import java.io.File;

public class PipelineDirectoryUtil {

  private static final String PIPELINE_BASE_DIR = "runInfo";

  public static File getPipelineDir(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
    File pipelineDir = new File(new File(new File(runtimeInfo.getDataDir(), PIPELINE_BASE_DIR), getEscapedPipelineName(pipelineName)), rev);
    if(!pipelineDir.exists()) {
      if(!pipelineDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", pipelineDir.getAbsolutePath()));
      }
    }
    return pipelineDir;
  }

  public static String getEscapedPipelineName(String pipelineName) {
    return pipelineName.replaceAll(" ", ".");
  }

}
