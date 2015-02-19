/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {

  KAFKA_00("Could not parse CVS, {}"),
  KAFKA_01("Could not parse JSON, {}"),

  //Kafka source and Target messages
  KAFKA_03("A message with offset '{}' which is greater than the latest offset '{}' is requested from Kafka topic '{}' partition '{}'"),
  KAFKA_04("Could not find new leader after kafka broker failure"),
  KAFKA_05("Error fetching offset data from the Broker '{}' : {}"),
  KAFKA_06("Can't find metadata for Topic '{}' and Partition '{}'"),
  KAFKA_07("Can't find leader for Topic '{}' and Partition '{}'"),
  KAFKA_08("Error communicating with Broker '{}' to find leader for topic '{}' partition '{}'. Reason : {}"),
  KAFKA_09("Error fetching data from kafka. Topic '{}', Partition '{}', Offset '{}"),
  KAFKA_10("Found old offset '{}'. Expected offset '{}'. Discarding message"),
  KAFKA_11("SocketTimeoutException encountered while fetching message from Kafka"),
  KAFKA_12("Error fetching data from kafka, {}"),
  KAFKA_13("Error fetching offset from kafka, {}"),
  KAFKA_14("Error creating Kafka consumer instance, {}"),
  KAFKA_15("Error getting iterator from Kafka Stream , {}"),

  KAFKA_16("Error writing data to kafka broker, {}"),
  KAFKA_17("Error serializing record, {}"),
  KAFKA_18("Could not retrieve metadata for topic '{}' from Broker '{}'. Reason : {}"),
  KAFKA_19("Could not find metadata for topic '{}' from the supplied metadata broker list '{}'"),
  KAFKA_20("Error evaluating partition expression '{}' for record '{}'. Reason {}"),
  KAFKA_21("Error converting the result of partition expression '{}' to a partition id for topic '{}'. Reason : {}"),
  KAFKA_22("Partition expression resulted in invalid partition id '{}'. Topic '{}' has {} partitions"),
  KAFKA_23("Failed to evaluate partition expression '{}', {}"),
  KAFKA_24("Topic '{}' does not exist."),

  ;
  private final String msg;

  Errors(String msg) {
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
