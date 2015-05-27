/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.google.common.collect.ImmutableSet;

public class ClusterModeConstants {
  public static final String API_LIB = "api-lib";
  public static final String CONTAINER_LIB = "container-lib";
  public static final String STREAMSETS_LIBS = "streamsets-libs";
  public static final String USER_LIBS = "user-libs";

  public static final String NUM_EXECUTORS_KEY = "num-executors";

  public static final String SPARK_KAFKA_JAR_PREFIX = "spark-streaming-kafka";
  /**
   * These jars are needed for preview and validate but cannot be shipped to the cluster
   */
  public static final ImmutableSet<String> EXCLUDED_JAR_PREFIXES = ImmutableSet.of("slf4j", "log4j", "scala",
    SPARK_KAFKA_JAR_PREFIX);

}
