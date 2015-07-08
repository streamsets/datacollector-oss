/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import com.streamsets.pipeline.spark.SparkStreamingBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class BootstrapClusterStreaming {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapClusterStreaming.class);

  /**
   * Bootstrapping the Driver which starts a Spark job on cluster
   */
  public static void main(String[] args) throws Exception {
    SparkStreamingBinding binding = null;
    try {
      binding = new SparkStreamingBinding(BootstrapCluster.getProperties());
      binding.init();
      binding.awaitTermination();
    } catch (Throwable error) {
      String msg = "Error trying to invoke BootstrapClusterStreaming.main: " + error;
      System.err.println(new Date()+ ": " + msg);
      error.printStackTrace(System.err); // required as in local mode the following seems to be lost
      LOG.error(msg, error);
      throw new IllegalStateException(msg, error);
    } finally {
      try {
        if (binding != null) {
          binding.close();
        }
      } catch (Exception ex) {
        LOG.warn("Error on binding close: " + ex, ex);
      }
    }
  }


}
