/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  public static final String CLUSTER_HDFS_CONFIG_BEAN_PREFIX = "clusterHDFSConfigBean.";
  public static final String CLUSTER_HDFS_DATA_FORMAT_CONFIG_PREFIX = CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "dataFormatConfig.";
  public static final String KAFKA_CONFIG_BEAN_PREFIX = "kafkaConfigBean.";
  public static final String KAFKA_DATA_FORMAT_CONFIG_BEAN_PREFIX = KAFKA_CONFIG_BEAN_PREFIX + "dataFormatConfig.";
  public static final String MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX = "maprstreamsSourceConfigBean.";
  public static final String MAPR_STREAMS_DATA_FORMAT_CONFIG_BEAN_PREFIX = MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX +
      "dataFormatConfig.";

  private Utils() {}

  public static <T> T checkNotNull(T value, Object varName) {
    if (value == null) {
      throw new NullPointerException(varName + " cannot be null");
    }
    return value;
  }

  public static <T> T  checkArgumentNotNull(T arg, Object msg) {
    if (arg == null) {
      throw new IllegalArgumentException((msg != null) ? msg.toString() : "");
    }
    return arg;
  }

  public static String getPropertyOrEmptyString(Properties properties, String name) {
    return properties.getProperty(name, "").trim();
  }

  public static String getPropertyNotNull(Properties properties, String name) {
    String val = checkArgumentNotNull(properties.getProperty(name), "Property " + name + " cannot be null");
    LOG.info("Value of property: " + name + " is " + val);
    return val.trim();
  }

  public static String getHdfsDataFormat(Properties properties) {
    return getPropertyNotNull(properties, CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "dataFormat");
  }

  public static String getHdfsCsvHeader(Properties properties) {
    return getPropertyOrEmptyString(properties, CLUSTER_HDFS_DATA_FORMAT_CONFIG_PREFIX + "csvHeader");
  }

  public static int getHdfsMaxBatchSize(Properties properties) {
    String maxBatchAsString = properties.getProperty(CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "maxBatchSize", "1000").trim();
    try {
      return Integer.parseInt(maxBatchAsString);
    } catch (NumberFormatException e) {
      String msg = "Invalid " + CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "maxBatchSize '" + maxBatchAsString + "' : " + e;
      throw new IllegalArgumentException(msg, e);
    }
  }

  public static String getKafkaTopic(Properties properties) {
    return getPropertyNotNull(properties, KAFKA_CONFIG_BEAN_PREFIX + "topic");
  }

  public static String getMaprStreamsTopic(Properties properties) {
    return getPropertyNotNull(properties, MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "topic");
  }

  public static String getMaprStreamsGroupId(Properties properties) {
    return getPropertyNotNull(properties, MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "consumerGroup");
  }

  public static String getKafkaMetadataBrokerList(Properties properties) {
    return getPropertyNotNull(properties, KAFKA_CONFIG_BEAN_PREFIX + "metadataBrokerList");
  }

  public static int getKafkaMaxBatchSize(Properties properties) {
    String maxBatchAsString = properties.getProperty(KAFKA_CONFIG_BEAN_PREFIX + "maxBatchSize", "1000").trim();
    try {
      return Integer.parseInt(maxBatchAsString);
    } catch (NumberFormatException e) {
      String msg = "Invalid " + KAFKA_CONFIG_BEAN_PREFIX + "maxBatchSize '" + maxBatchAsString + "' : " + e;
      throw new IllegalArgumentException(msg, e);
    }
  }

  public static int getMaprStreamsMaxBatchSize(Properties properties) {
    String maxBatchAsString = properties.getProperty(MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "maxBatchSize",
        "1000"
    ).trim();
    try {
      return Integer.parseInt(maxBatchAsString);
    } catch (NumberFormatException e) {
      String msg = "Invalid " + MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "maxBatchSize '" + maxBatchAsString + "' : "
          + e;
      throw new IllegalArgumentException(msg, e);
    }
  }

  public static long getKafkaMaxWaitTime(Properties properties) {
    String durationAsString = properties.getProperty(KAFKA_CONFIG_BEAN_PREFIX + "maxWaitTime", "2000").trim();
    try {
      return Long.parseLong(durationAsString);
    } catch (NumberFormatException e) {
      String msg = "Invalid " + KAFKA_CONFIG_BEAN_PREFIX + "maxWaitTime '" + durationAsString + "' : " + e;
      throw new IllegalArgumentException(msg, e);
    }
  }

  public static long getMaprStreamsWaitTime(Properties properties) {
    String durationAsString = properties.getProperty(
        MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "maxWaitTime",
        "2000"
    ).trim();
    try {
      return Long.parseLong(durationAsString);
    } catch (NumberFormatException e) {
      String msg = "Invalid " + MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "maxWaitTime '" + durationAsString + "' : "
          + e;
      throw new IllegalArgumentException(msg, e);
    }
  }
}
