/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.impl;

import java.util.List;
import java.util.Map;

/**
 * Common interface for cluster mode sources which push data.
 */
public interface ClusterFunction {

  /**
   * Invoke pipeline to process a batch. List passed to this interface
   * may not be reused after this call returns.
   */
  void invoke(List<Map.Entry> batch) throws Exception;

  void shutdown() throws Exception;
}
