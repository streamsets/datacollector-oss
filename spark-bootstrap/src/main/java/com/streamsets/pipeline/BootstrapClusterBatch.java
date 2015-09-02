/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
