/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline;

import com.streamsets.pipeline.hadoop.HadoopMapReduceBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapClusterBatch {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapClusterBatch.class);

  private BootstrapClusterBatch() {}
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
