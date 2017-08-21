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
package com.streamsets.pipeline.kafka.impl;

public class Kafka09Constants {
  public static final String KAFKA_VERSION = "0.9";

  // Producer related Constants
  public static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
  public static final String KEY_SERIALIZER_KEY = "key.serializer";
  public static final String VALUE_SERIALIZER_KEY = "value.serializer";

  private Kafka09Constants() {}
}
