/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

public enum State {

  /**
   * The pipeline was stopped by a user or an error.
   */
  STOPPED,
  RUNNING,
  STOPPING,
  ERROR,
  /**
   * The origin returned null as offset and is done consuming data, forever. The pipeline stopped naturally.
   */
  FINISHED,
  NODE_PROCESS_SHUTDOWN
}
