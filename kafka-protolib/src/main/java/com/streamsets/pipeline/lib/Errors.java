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
  KAFKA_03("Could not find metadata for topic '{}' from the supplied metadata broker list '{}'"),
  KAFKA_04("Topic '{}' does not exist."),
  KAFKA_05("Topic cannot be empty."),
  KAFKA_06("Broker URIs cannot be empty."),
  KAFKA_07("Invalid Broker URI '{}'."),

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
  KAFKA_33("Consumer Group cannot be empty"),
  KAFKA_34("Max Batch Size (messages) cannot be less than 1"),
  KAFKA_35("Batch Wait Time (millisecs) cannot be less than 1"),
  KAFKA_36("Invalid XML element name '{}'"),
  KAFKA_37("Could not parse record from message '{}', {}"),
  KAFKA_38("Max data object length cannot be less than 1"),
  KAFKA_39("Unsupported data format '{}'"),


  //Kafka target messages
  KAFKA_50("Error writing data to kafka broker, {}"),
  KAFKA_51("Error serializing record '{}', {}"),
  KAFKA_52("Could not retrieve metadata for topic '{}' from Broker '{}'. Reason : {}"),
  KAFKA_54("Error evaluating partition expression '{}' for record '{}'. Reason {}"),
  KAFKA_55("Error converting the result of partition expression '{}' to a partition id for topic '{}'. Reason : {}"),
  KAFKA_56("Partition expression resulted in invalid partition id '{}'. Topic '{}' has {} partitions. Record '{}'"),
  KAFKA_57("Invalid partition expression '{}', {}"),
  KAFKA_58("Field cannot be empty"),
  KAFKA_59("Fields cannot be empty"),
  KAFKA_60("Could not serialize record '{}', all records from batch '{}' for partition '{}' are sent to error, error: {}"),

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
