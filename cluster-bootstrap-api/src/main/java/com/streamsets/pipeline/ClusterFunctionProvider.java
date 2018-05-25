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

import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.pipeline.impl.ClusterFunction;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;

public class ClusterFunctionProvider {

  private static ClusterFunction clusterFunction;

  private ClusterFunctionProvider() {

  }

  public static synchronized ClusterFunction getClusterFunction() throws Exception {
    // Why such a complex name?
    // When an executor dies and a new one takes its place, having just partition id won't work, because the old file
    // might not have been closed by the namenode since the old executor handling that partition may have just died.
    // So we must ensure a truly unique part which is executor id.
    // ---- BUT ----
    // Multiple partitions of the same job can run on the same executor, which is especially true now since we allow
    // the user to set fewer executors than partitions, so we need the partition id.
    // ---- BUT ----
    // Users could end up not making it unique enough, since partition id and executor id are not unique across jobs, so
    // if they use ${sdc:id()} in 2 cluster pipelines with same directory, then it will still collide, so prefix this
    // with pipeline id.
    // ---- DONE, YAY! ----
    if (clusterFunction == null) {
      clusterFunction =
          (ClusterFunction) BootstrapCluster.getClusterFunction(
              BootstrapCluster.getProperties().getProperty(ClusterModeConstants.CLUSTER_PIPELINE_NAME) +
                  "-" +
                  TaskContext.get().partitionId() + "-" +
                  SparkEnv.get().executorId()
          );
    }
    return clusterFunction;
  }
}
