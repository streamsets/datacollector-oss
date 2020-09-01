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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  // we cache a split version of the templates to speed up formatting
  private static final Map<String, String[]> TEMPLATES = new ConcurrentHashMap<>();

  private static final String TOKEN = "{}";

  public static final String CLUSTER_HDFS_CONFIG_BEAN_PREFIX = "clusterHDFSConfigBean.";
  public static final String CLUSTER_HDFS_DATA_FORMAT_CONFIG_PREFIX = CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "dataFormatConfig.";
  public static final String KAFKA_CONFIG_BEAN_PREFIX = "kafkaConfigBean.";
  public static final String KAFKA_CONNECTION_CONFIG_BEAN_PREFIX = "kafkaConfigBean.connectionConfig.connection.";
  public static final String KAFKA_DATA_FORMAT_CONFIG_BEAN_PREFIX = KAFKA_CONFIG_BEAN_PREFIX + "dataFormatConfig.";
  public static final String MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX = "maprstreamsSourceConfigBean.";
  public static final String MAPR_STREAMS_DATA_FORMAT_CONFIG_BEAN_PREFIX = MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX +
      "dataFormatConfig.";
  private static final String NO_OF_PARTITIONS = "partitionCount";

  private Utils() {}

  public static <T> T checkNotNull(T value, Object varName) {
    if (value == null) {
      throw new NullPointerException(varName + " cannot be null");
    }
    return value;
  }

  public static void checkArgument(boolean expression, String msg, Object ... params) {
    if (!expression) {
      throw new IllegalArgumentException((msg != null) ? format(msg, params) : "");
    }
  }

  public static void checkState(boolean expression, String msg, Object... params) {
    if (!expression) {
      throw new IllegalStateException((msg != null) ? format(msg, params) : "");
    }
  }

  static String[] prepareTemplate(String template) {
    List<String> list = new ArrayList<>();
    int pos = 0;
    int nextToken = template.indexOf(TOKEN, pos);
    while (nextToken > -1 && pos < template.length()) {
      list.add(template.substring(pos, nextToken));
      pos = nextToken + TOKEN.length();
      nextToken = template.indexOf(TOKEN, pos);
    }
    list.add(template.substring(pos));
    return list.toArray(new String[list.size()]);
  }

  // fast version of SLF4J MessageFormat.format(), uses {} tokens,
  // no escaping is supported, no array content printing either.
  public static String format(String template, Object... args) {
    String[] templateArr = TEMPLATES.get(template);
    if (templateArr == null) {
      // we may have a race condition here but the end result is idempotent
      templateArr = prepareTemplate(template);
      TEMPLATES.put(template, templateArr);
    }
    StringBuilder sb = new StringBuilder(template.length() * 2);
    for (int i = 0; i < templateArr.length; i++) {
      sb.append(templateArr[i]);
      if (args != null && (i < templateArr.length - 1)) {
        sb.append((i < args.length) ? args[i] : TOKEN);
      }
    }
    return sb.toString();
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

  public static int getKafkaPartitionRateLimit(Properties properties) {
    String rateLimitAsString = properties.getProperty(KAFKA_CONFIG_BEAN_PREFIX + "maxRatePerPartition", "1000").trim();
    try {
      return Integer.parseInt(rateLimitAsString);
    } catch (NumberFormatException e) {
      String msg = "Invalid " + KAFKA_CONFIG_BEAN_PREFIX + "maxRatePerPartition '" + rateLimitAsString + "' : " + e;
      throw new IllegalArgumentException(msg, e);
    }
  }

  public static String getKafkaTopic(Properties properties) {
    return getPropertyNotNull(properties, KAFKA_CONFIG_BEAN_PREFIX + "topic");
  }

  public static String getKafkaConsumerGroup(Properties properties) {
    return getPropertyNotNull(properties, KAFKA_CONFIG_BEAN_PREFIX + "consumerGroup");
  }

  public static String getMaprStreamsTopic(Properties properties) {
    return getPropertyNotNull(properties, MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "topic");
  }

  public static String getMaprStreamsConsumerGroup(Properties properties) {
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

  public static int getMaprStreamsRateLimit(Properties properties) {
    String maxRateAsString = properties.getProperty(MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "maxRatePerPartition",
        "1000").trim();
    try {
      return Integer.parseInt(maxRateAsString);
    } catch (NumberFormatException e) {
      String msg = "Invalid " + MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "maxRatePerPartition '" + maxRateAsString + "' : "
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

  public static Map<String, String> getExtraKafkaConfigs(Properties properties) {
    return properties.entrySet().stream().filter(e -> e.getKey().toString().startsWith(ClusterModeConstants
        .EXTRA_KAFKA_CONFIG_PREFIX)).collect(
        Collectors.toMap(
            e -> e.getKey().toString().replaceFirst(ClusterModeConstants.EXTRA_KAFKA_CONFIG_PREFIX, ""),
            e -> e.getValue().toString()
        ));
  }

  public static int getNumberOfPartitions(Properties properties) {
    String numExecutors = properties.getProperty(NO_OF_PARTITIONS).trim();
    try {
      return Integer.parseInt(numExecutors);
    } catch (NumberFormatException e) {
      String msg = "Invalid parallelism/Number of executors : "
          + e;
      throw new IllegalArgumentException(msg, e);
    }
  }
}
