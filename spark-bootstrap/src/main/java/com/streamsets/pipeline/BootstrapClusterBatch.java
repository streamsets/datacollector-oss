/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import com.streamsets.pipeline.hadoop.HadoopMapReduceBinding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BootstrapClusterBatch {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapClusterBatch.class);
  /**
   * Bootstrapping the Driver which starts a Hadoop job on cluster
   */
  public static void main(String[] args) throws Exception {
    HadoopMapReduceBinding binding = null;
    try {
      binding = new HadoopMapReduceBinding(args);
      binding.init();
      binding.awaitTermination(); // killed by ClusterProviderImpl before returning
    } catch (Exception ex) {
      String msg = "Error trying to invoke BootstrapClusterBatch.main: " + ex;
      throw new IllegalStateException(msg, ex);
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
