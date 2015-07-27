/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution;

public interface PipelineListener {

  //called when the pipeline starts running
  public void start(String name, String rev);

  //called when the pipeline stops running
  //status has the exiting status from RUNNING (RUN_ERROR, STOPPING, FINISHING, DISCONNECTING)
  public void stop(String name, String rev, PipelineStatus status);

}
