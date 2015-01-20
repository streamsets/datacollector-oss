/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.ErrorCode;

public enum KafkaStageLibError implements ErrorCode {

  //Kafka source and Target messages
  KFK_0300("A message with offset '{}' which is greater than the latest offset '{}' is requested from Kafka topic '{}' partition '{}'."),
  KFK_0301("Could not find new leader after kafka broker failure."),
  KFK_0302("Error fetching offset data from the Broker '{}' : {}"),
  KFK_0303("Can't find metadata for Topic '{}' and Partition '{}'."),
  KFK_0304("Can't find leader for Topic '{}' and Partition '{}'."),
  KFK_0305("Error communicating with Broker '{}' to find leader for topic '{}' partition '{}'. Reason : {}"),
  KFK_0306("Error fetching data from kafka. Topic '{}', Partition '{}', Offset '{}."),
  KFK_0307("Found old offset '{}'. Expected offset '{}'. Discarding message"),
  KFK_0308("SocketTimeoutException encountered while fetching message from Kafka."),
  KFK_0309("Error fetching data from kafka, {}"),
  KFK_0310("Error fetching offset from kafka, {}"),
  KFK_0311("Error creating Kafka consumer instance, {}"),
  KFK_0312("Error getting iterator from Kafka Stream , {}"),

  KFK_0350("Error writing data to kafka broker, {}"),
  KFK_0351("Error serializing record, {}"),
  KFK_0352("Could not retrieve metadata for topic '{}' from Broker '{}'. Reason : {}"),
  KFK_0353("Could not find metadata for topic '{}' from the supplied metadata broker list '{}'."),
  KFK_0354("Error evaluating partition expression '{}' for record '{}'. Reason {}"),
  KFK_0355("Error converting the result of partition expression '{}' to a partition id for topic '{}'. Reason : {}"),
  KFK_0356("Partition expression resulted in invalid partition id '{}'. Topic '{}' has {} partitions."),
  KFK_0357("Failed to evaluate partition expression '{}', {}"),

  ;
  private final String msg;

  KafkaStageLibError(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
