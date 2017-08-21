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
package com.streamsets.pipeline.stage.origin.mongodb;

public class MongoDBSourceBuilder extends AbstractMongoDBSourceBuilder<MongoDBSource, MongoDBSourceBuilder>{

  public MongoDBSourceBuilder() {
    super();
    config.offsetField = "_id";
    config.initialOffset = "0";
    config.mongoConfig.database = "database";
    config.offsetType = OffsetFieldType.OBJECTID;
  }

  public MongoDBSourceBuilder database(String database) {
    config.mongoConfig.database = database;
    return this;
  }

  public MongoDBSourceBuilder isCapped(boolean isCapped) {
    config.isCapped = isCapped;
    return this;
  }

  public MongoDBSourceBuilder offsetField(String offset) {
    config.offsetField = offset;
    return this;
  }

  public MongoDBSourceBuilder initialOffset(String initialOffset) {
    config.initialOffset = initialOffset;
    return this;
  }

  public MongoDBSourceBuilder setOffsetType(OffsetFieldType type) {
    config.offsetType = type;
    return this;
  }

  @Override
  public MongoDBSource build() {
    return new MongoDBSource(config);
  }
}
