/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import java.util.Properties;

import com.streamsets.pipeline.spark.SparkBatchBinding;
import com.streamsets.pipeline.spark.SparkStreamingBinding;

public class DelegatingClusterBinding implements ClusterBinding {

  private final Properties properties;
  private final String pipelineJson;
  private final ClusterBinding clusterBinding;

  public DelegatingClusterBinding(Properties properties, String pipelineJson) {
    this.properties = Utils.checkNotNull(properties, "Properties");
    this.pipelineJson = Utils.checkNotNull(pipelineJson, "Pipeline JSON");
    if (!Boolean.valueOf(getProperty("cluster.source.batchmode"))) {
      clusterBinding = new SparkStreamingBinding(properties, pipelineJson);
    } else {
      clusterBinding = new SparkBatchBinding(properties, pipelineJson);
    }
  }

  private String getProperty(String name) {
    Utils.checkArgumentNotNull(properties.getProperty(name),
      "Property " + name +" cannot be null");
    return properties.getProperty(name).trim();
  }

  @Override
  public void init() throws Exception {
    clusterBinding.init();

  }

  @Override
  public void awaitTermination() {
    clusterBinding.awaitTermination();

  }

  @Override
  public void close() {
    clusterBinding.close();
  }
}
