/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

public class ShutdownObject {

  private volatile boolean stop = false;

  public boolean isStop() {
    return stop;
  }

  public void setStop(boolean stop) {
    this.stop = stop;
  }
}
