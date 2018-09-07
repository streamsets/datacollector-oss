/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.mongodb;

import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.common.mongodb.AuthenticationType;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;
import com.streamsets.pipeline.stage.origin.mongodb.ReadPreferenceLabel;
import com.streamsets.pipeline.stage.processor.kv.EvictionPolicyType;

import java.util.LinkedList;

public class MongoDBProcessorBuilder {

  MongoDBProcessorConfigBean config;

  public MongoDBProcessorBuilder() {
    config = new MongoDBProcessorConfigBean();
    config.mongoConfig = new MongoDBConfig();
    config.fieldMapping = new LinkedList<>();
    config.multipleValuesBehavior = MultipleValuesBehavior.DEFAULT;
    config.missingValuesBehavior = MissingValuesBehavior.DEFAULT.DEFAULT;
    config.mongoConfig.authenticationType = AuthenticationType.NONE;
    config.cacheConfig.enabled = false;
    config.cacheConfig.evictionPolicyType = EvictionPolicyType.EXPIRE_AFTER_WRITE;
    config.readPreference = ReadPreferenceLabel.NEAREST;
  }

  public MongoDBProcessorBuilder database(String db) {
    config.mongoConfig.database = db;
    return this;
  }

  public MongoDBProcessorBuilder collection(String col) {
    config.mongoConfig.collection = col;
    return this;
  }

  public MongoDBProcessorBuilder connectionString(String url) {
    config.mongoConfig.connectionString = url;
    return this;
  }

  public MongoDBProcessorBuilder addFieldMapping(MongoDBFieldColumnMapping mapping) {
    config.fieldMapping.add(mapping);
    return this;
  }

  public MongoDBProcessorBuilder resultField(String field) {
    config.resultField = field;
    return this;
  }

  public MongoDBProcessor build() {
    return new MongoDBProcessor(config);
  }
}
