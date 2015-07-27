/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner.production;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;

import java.io.File;

public class OffsetFileUtil {

  private static final String OFFSET_FILE = "offset.json";

  public static File getPipelineOffsetFile(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), OFFSET_FILE);
  }
}
