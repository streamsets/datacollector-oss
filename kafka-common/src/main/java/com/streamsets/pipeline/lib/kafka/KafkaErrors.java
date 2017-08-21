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
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum KafkaErrors implements ErrorCode {

  KAFKA_03("Cannot find metadata for topic '{}' from the broker list '{}'"),
  KAFKA_04("Topic '{}' does not exist"),
  KAFKA_05("Topic cannot be empty"),
  KAFKA_06("Zookeeper and broker URIs cannot be empty"),
  KAFKA_07("Invalid broker URI '{}'"),
  KAFKA_09("Invalid Zookeeper connect string '{}' : {}"),
  KAFKA_10("Cannot validate configuration: {}"),
  KAFKA_11("Cannot retrieve metadata for topic '{}' from broker '{}': {}"),

  //Kafka source messages
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
  KAFKA_35("Batch wait time cannot be less than 1"),
  KAFKA_37("Cannot parse record from message '{}': {}"),
  KAFKA_40("Messages with XML data cannot have multiple XML documents in a single message"),
  KAFKA_41("Could not get partition count for topic '{}' : {}"),
  KAFKA_42("Could not get partition count for topic '{}'"),
  KAFKA_43("Schema Registry URLs must be configured to use Confluent Deserializer"),
  KAFKA_44("Confluent Avro Deserializer not supported by this version of Kafka."),
  KAFKA_74("Message payload cannot be null"),

  //Kafka target messages
  KAFKA_50("Error writing data to the Kafka broker: {}"),
  KAFKA_51("Error serializing record '{}': {}"),
  KAFKA_54("Error evaluating the partition expression '{}' for record '{}': {}"),
  KAFKA_55("Error converting the partition expression '{}' to a partition ID for topic '{}': {}"),
  KAFKA_56("Partition expression generated an invalid partition ID '{}'. Topic '{}' has {} partitions. Record '{}'."),
  KAFKA_57("Invalid partition expression '{}': {}"),
  KAFKA_58("Field cannot be empty"),
  KAFKA_59("Fields cannot be empty"),
  KAFKA_60("Cannot serialize record '{}'. All records from batch with entity '{}' and offset '{}' for partition '{}' are sent to error: {}"),
  KAFKA_61("Invalid topic expression '{}': {}"),
  KAFKA_62("Topic expression '{}' generated a null or empty topic for record '{}'"),
  KAFKA_63("Error evaluating topic expression '{}' for record '{}': {}"),
  KAFKA_64("Topic White List cannot be empty if topic is resolved at runtime"),
  KAFKA_65("Topic '{}' resolved from record '{}' is not among the allowed topics"),
  KAFKA_66("Kafka Producer configuration '{}' must be specified a valid {} value greater than or equal to 0"),
  KAFKA_67("Error connecting to Kafka Brokers '{}'"),
  KAFKA_68("Error getting metadata for topic '{}' from broker '{}' due to error: {}"),
  KAFKA_69("Message is larger than the maximum allowed size configured in Kafka Broker"),
  KAFKA_70("Include Schema cannot be used in conjunction with Confluent Serializer"),
  KAFKA_71("Schema Registry URLs must be configured to use Confluent Serializer"),
  KAFKA_72("Subject or Schema ID must be defined to use Confluent Serializer"),
  KAFKA_73("Confluent Avro Serializer not supported by this version of Kafka."),
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
