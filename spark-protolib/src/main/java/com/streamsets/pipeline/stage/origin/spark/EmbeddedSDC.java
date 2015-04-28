/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;


import java.util.concurrent.atomic.AtomicInteger;

/**
 * Embedded SDC providing access to the source
 *
 */
public class EmbeddedSDC {
  private static final AtomicInteger instanceIdCounter = new AtomicInteger(0);
  private final int instanceId;
  private SparkStreamingSource source;

  public EmbeddedSDC() {
    instanceId = instanceIdCounter.getAndIncrement();
  }

  public int getInstanceId() {
    return instanceId;
  }

  public SparkStreamingSource getSource() {
    return source;
  }

  public void setSource(SparkStreamingSource source) {
    this.source = source;
  }
}
