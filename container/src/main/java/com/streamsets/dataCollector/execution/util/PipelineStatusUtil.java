/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.util;

import com.streamsets.dataCollector.execution.PipelineStatus;

public class PipelineStatusUtil {

  public static boolean isActive(PipelineStatus pipelineStatus) {
    switch (pipelineStatus) {
      case EDITED:
      case FINISHED:
      case KILLED:
      case RUN_ERROR:
      case START_ERROR:
      case STOPPED:
        return false;
      default:
        return true;
    }
  }

}
