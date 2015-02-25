/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {

  KAFKA_00("Could not parse CVS, {}"),
  KAFKA_01("Could not parse JSON, {}"),
  KAFKA_02("Unsupported data format '{}'"),
  KAFKA_03("Invalid value '{}' specified for configuration '{}'"),
  KAFKA_04("No value specified for configuration '{}'"),
  KAFKA_05("Could not find metadata for topic '{}' from the supplied metadata broker list '{}'"),
  KAFKA_06("Topic '{}' does not exist."),

  //Kafka source messages
  KAFKA_20("A message with offset '{}' which is greater than the latest offset '{}' is requested from Kafka topic '{}' partition '{}'"),
  KAFKA_21("Could not find new leader after kafka broker failure"),
  KAFKA_22("Error fetching offset data from the Broker '{}' : {}"),
  KAFKA_23("Can't find metadata for Topic '{}' and Partition '{}'"),
  KAFKA_24("Can't find leader for Topic '{}' and Partition '{}'"),
  KAFKA_25("Error communicating with Broker '{}' to find leader for topic '{}' partition '{}'. Reason : {}"),
  KAFKA_26("Error fetching data from kafka. Topic '{}', Partition '{}', Offset '{}"),
  KAFKA_27("Found old offset '{}'. Expected offset '{}'. Discarding message"),
  KAFKA_28("SocketTimeoutException encountered while fetching message from Kafka"),
  KAFKA_29("Error fetching data from kafka, {}"),
  KAFKA_30("Error fetching offset from kafka, {}"),
  KAFKA_31("Error connecting to zookeeper with connect string '{}', {}"),
  KAFKA_32("Error getting iterator from Kafka Stream , {}"),

  //Kafka target messages
  KAFKA_50("Error writing data to kafka broker, {}"),
  KAFKA_51("Error serializing record, {}"),
  KAFKA_52("Could not retrieve metadata for topic '{}' from Broker '{}'. Reason : {}"),
  KAFKA_54("Error evaluating partition expression '{}' for record '{}'. Reason {}"),
  KAFKA_55("Error converting the result of partition expression '{}' to a partition id for topic '{}'. Reason : {}"),
  KAFKA_56("Partition expression resulted in invalid partition id '{}'. Topic '{}' has {} partitions"),
  KAFKA_57("Failed to evaluate partition expression '{}', {}"),

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
