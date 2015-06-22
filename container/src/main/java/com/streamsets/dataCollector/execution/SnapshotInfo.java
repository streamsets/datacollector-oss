/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution;

public interface SnapshotInfo {

  public String getId();

  public String getName();

  public String getRev();

  public long getTimeStamp();

  public String getUser();

  public boolean isInProgress();

}
