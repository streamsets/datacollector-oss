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

public class KafkaConstants {
  public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
  public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
  public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
  public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
  public static final String CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
  public static final String BASIC_AUTH_CREDENTIAL_SOURCE = "basic.auth.credentials.source";
  public static final String BASIC_AUTH_USER_INFO = "basic.auth.user.info";
  public static final String USER_INFO = "USER_INFO";

  // Constants specific for new consumer/producer library
  public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
  public static final String AUTO_OFFSET_RESET_PREVIEW_VALUE = "earliest";
  public static final String AUTO_COMMIT_OFFEST = "enable.auto.commit";

}
