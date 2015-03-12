/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

public interface SourceOffsetTracker {

  public boolean isFinished();

  public String getOffset();

  public void setOffset(String newOffset);

  public void commitOffset();

  public long getLastBatchTime();

}
