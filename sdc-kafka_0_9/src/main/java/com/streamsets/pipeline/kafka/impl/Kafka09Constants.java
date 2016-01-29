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
package com.streamsets.pipeline.kafka.impl;

public class Kafka09Constants {
  public static final String KAFKA_VERSION = "0.9";

  // Producer related Constants
  public static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
  public static final String KEY_SERIALIZER_KEY = "key.serializer";
  public static final String KEY_SERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringSerializer";
  public static final String VALUE_SERIALIZER_KEY = "value.serializer";
  public static final String VALUE_SERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.ByteArraySerializer";
  public static final String REQUEST_REQUIRED_ACKS_KEY = "acks";
  public static final String REQUEST_REQUIRED_ACKS_DEFAULT = "1";

  public static final String PARTITIONER_CLASS_KEY = "partitioner.class";
  public static final String RANDOM_PARTITIONER_CLASS = "com.streamsets.pipeline.kafka.impl.RandomPartitioner";
  public static final String ROUND_ROBIN_PARTITIONER_CLASS = "com.streamsets.pipeline.kafka.impl.RoundRobinPartitioner";
  public static final String EXPRESSION_PARTITIONER_CLASS = "com.streamsets.pipeline.kafka.impl.ExpressionPartitioner";

  private Kafka09Constants() {}
}
