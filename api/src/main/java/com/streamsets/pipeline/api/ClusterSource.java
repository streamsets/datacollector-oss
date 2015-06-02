/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import java.util.List;
import java.util.Map;

/**
 * Cluster source which should be implemented by any source which can run in cluster mode
 */
public interface ClusterSource extends Source {

  /**
   * Writes batch of data to the source
   * @param batch
   * @throws InterruptedException
   */
  <T> void put(List<T> batch) throws InterruptedException;

  /**
   * Return the no of records produced by this source
   * @return
   */
  long getRecordsProduced();

  /**
   * Return true if a unrecoverable error has occured
   * @return
   */
  boolean inErrorState();

  /**
   * Returns name of this origin
   * @return
   */
  String getName();

  /**
   * Whether source is configured to run in batch mode or not
   * @return
   */
  boolean isInBatchMode();

  /**
   * The configs to ship to cluster
   * @return
   */
  Map<String, String> getConfigsToShip();
}
