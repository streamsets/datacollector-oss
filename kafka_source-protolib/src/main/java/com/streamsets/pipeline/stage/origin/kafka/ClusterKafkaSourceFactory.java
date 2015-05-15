/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

public class ClusterKafkaSourceFactory extends KafkaSourceFactory {
  private static final String CLUSTER_MODE_CLASS = "com.streamsets.pipeline.stage.origin.kafka.cluster.ClusterKafkaSource";
  public ClusterKafkaSourceFactory(SourceArguments args) {
    super(args);
  }

  public BaseKafkaSource create() {
    try {
      Class clusterModeClazz = Class.forName(CLUSTER_MODE_CLASS);
      return (BaseKafkaSource) clusterModeClazz.getConstructor(SourceArguments.class)
        .newInstance(new Object[]{args});
    } catch (Exception e) {
      throw new IllegalStateException("Exception while invoking kafka instance in cluster mode: " + e, e);
    }
  }
}
