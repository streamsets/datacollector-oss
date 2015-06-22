/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution;

public enum PreviewStatus {
  VALIDATING,     // validating the configuration, during preview
  VALID,          // configuration is valid, during preview
  INVALID,        // configuration is invalid, during preview

  STARTING,       // preview starting (initialization)
  START_ERROR,    // preview failed while start (during initialization)
  RUNNING,        // preview running
  RUN_ERROR,      // preview failed while running

  FINISHING,      // preview finishing (calling destroy on pipeline)
  FINISHED,       // preview finished  (done)

  CANCELLING,     // preview has been manually stopped
  CANCELLED,      // preview has been manually stopped
  ;
  
}
