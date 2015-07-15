/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution;

// Implementations of this interface will be registered for periodic execution (via the run() method) at the
// specified frequency.
// Components like SnapshotStore and PipelineStateStore which need to purge old data should implement this
// interface.
public interface HouseKeeper extends Runnable {

  public long getDelayToFirstRunMs();

  public long getFrequencyMs();

}
