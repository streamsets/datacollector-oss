/*
 * Copyright 2018 StreamSets Inc.
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
import com.streamsets.pipeline.spark.SparkStreamingBinding;
import com.streamsets.pipeline.spark.SparkStreamingBindingFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class BootstrapClusterStreaming {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapClusterStreaming.class);

  private BootstrapClusterStreaming() {}

  /**
   * Bootstrapping the Driver which starts a Spark job on cluster
   */
  public static void main(String[] args) throws Exception {
    SparkStreamingBinding binding = null;
    try {
      binding = SparkStreamingBindingFactory.build(BootstrapCluster.getProperties());
      binding.init();
      BootstrapCluster.createTransformers(binding.getStreamingContext().sparkContext(), binding.getSparkSession());
      binding.startContext();
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
