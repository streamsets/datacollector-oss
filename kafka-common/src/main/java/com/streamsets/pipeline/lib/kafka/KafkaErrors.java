/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum KafkaErrors implements ErrorCode {

  KAFKA_00("Cannot parse CSV: {}"),
  KAFKA_01("Cannot parse JSON: {}"),
  KAFKA_02("Unsupported data format '{}'"),
  KAFKA_03("Cannot find metadata for topic '{}' from the broker list '{}'"),
  KAFKA_04("Topic '{}' does not exist"),
  KAFKA_05("Topic cannot be empty"),
  KAFKA_06("Zookeeper URI cannot be empty"),
  KAFKA_07("Invalid broker URI '{}'"),
  KAFKA_08("Unsupported charset '{}'"),
  KAFKA_09("Invalid Zookeeper connect string '{}' : {}"),
  KAFKA_10("Cannot validate configuration: {}"),

  //Kafka source messages
    /* LC Hari says the first one is not being used right now  */
  KAFKA_20("A message with offset '{}' which is greater than the latest offset '{}' is requested from Kafka topic '{}' partition '{}'"),
  KAFKA_21("Cannot find a new leader after a Kafka broker failure"),
  KAFKA_22("Error fetching offset data from the broker '{}': {}"),
  KAFKA_23("Cannot find metadata for topic '{}', partition '{}'"),
  KAFKA_24("Cannot find leader for topic '{}', partition '{}'"),
  KAFKA_25("Error communicating with broker '{}' to find leader for topic '{}', partition '{}': {}"),
  KAFKA_26("Error fetching data from Kafka topic '{}', partition '{}', offset '{}"),
  KAFKA_27("Found old offset '{}'. Expected offset '{}'. Discarding message."),
  KAFKA_28("SocketTimeoutException encountered while fetching message from Kafka"),
  KAFKA_29("Error fetching data from Kafka: {}"),
  KAFKA_30("Error fetching offset from Kafka: {}"),
  KAFKA_31("Error connecting to ZooKeeper with connection string '{}': {}"),
  KAFKA_32("Error getting iterator from KafkaStream: {}"),
  KAFKA_33("Define the consumer group"),
  KAFKA_34("Max batch size cannot be less than 1"),
  KAFKA_35("Batch wait time cannot be less than 1"),
  KAFKA_36("Invalid XML element name '{}'"),
  KAFKA_37("Cannot parse record from message '{}': {}"),
  KAFKA_38("Max data object length cannot be less than 1"),
  KAFKA_39("Unsupported data format '{}'"),
  KAFKA_40("Messages with XML data cannot have multiple XML documents in a single message"),
  KAFKA_41("Could not get partition count for topic '{}' : {}"),
  KAFKA_42("Could not get partition count for topic '{}'"),
  KAFKA_43("Avro Schema must be specified"),

  //Kafka target messages
  KAFKA_50("Error writing data to the Kafka broker: {}"),
  KAFKA_51("Error serializing record '{}': {}"),
  KAFKA_52("Cannot retrieve metadata for topic '{}' from broker '{}': {}"),
  KAFKA_54("Error evaluating the partition expression '{}' for record '{}': {}"),
  KAFKA_55("Error converting the partition expression '{}' to a partition ID for topic '{}': {}"),
  KAFKA_56("Partition expression generated an invalid partition ID '{}'. Topic '{}' has {} partitions. Record '{}'."),
  KAFKA_57("Invalid partition expression '{}': {}"),
  KAFKA_58("Field cannot be empty"),
  KAFKA_59("Fields cannot be empty"),
  KAFKA_60("Cannot serialize record '{}'. All records from batch '{}' for partition '{}' are sent to error: {}"),
  KAFKA_61("Invalid topic expression '{}': {}"),
  KAFKA_62("Topic expression '{}' generated a null or empty topic for record '{}'"),
  KAFKA_63("Error evaluating topic expression '{}' for record '{}': {}"),
  KAFKA_64("Topic White List cannot be empty if topic is resolved at runtime"),
  KAFKA_65("Topic '{}' resolved from record '{}' is not among the allowed topics"),
  KAFKA_66("Kafka Producer configuration '{}' must be specified a valid {} value greater than or equal to 0"),
  KAFKA_67("Error connecting to Kafka Brokers '{}'"),


  ;
  private final String msg;

  KafkaErrors(String msg) {
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
