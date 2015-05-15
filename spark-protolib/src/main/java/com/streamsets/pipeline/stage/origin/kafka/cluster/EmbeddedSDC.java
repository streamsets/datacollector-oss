/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka.cluster;


import java.util.concurrent.atomic.AtomicInteger;

/**
 * Embedded SDC providing access to the source
 *
 */
public class EmbeddedSDC {
  private static final AtomicInteger instanceIdCounter = new AtomicInteger(0);
  private final int instanceId;
  private ClusterSource source;

  public EmbeddedSDC() {
    instanceId = instanceIdCounter.getAndIncrement();
  }

  public int getInstanceId() {
    return instanceId;
  }

  public ClusterSource getSource() {
    return source;
  }

  public void setSource(ClusterSource source) {
    this.source = source;
  }

  public boolean inErrorState() {
    return source != null && source.inErrorState();
  }
}
