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
package com.streamsets.pipeline.stage.origin.mongodb.oplog;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.stage.origin.mongodb.AbstractMongoDBSourceBuilder;

import java.util.List;

public class MongoDBOplogSourceBuilder extends AbstractMongoDBSourceBuilder<MongoDBOplogSource, MongoDBOplogSourceBuilder> {
  private MongoDBOplogSourceConfigBean mongoDBOplogSourceConfigBean;

  public MongoDBOplogSourceBuilder() {
    super();
    config.mongoConfig.database = "local";
    mongoDBOplogSourceConfigBean = new MongoDBOplogSourceConfigBean();
    mongoDBOplogSourceConfigBean.initialTs = -1;
    mongoDBOplogSourceConfigBean.initialOrdinal = -1;
    mongoDBOplogSourceConfigBean.filterOplogOpTypes = Lists.newArrayList(OplogOpType.INSERT, OplogOpType.UPDATE, OplogOpType.DELETE);
  }

  public MongoDBOplogSourceBuilder initialTs(int initialTs) {
    mongoDBOplogSourceConfigBean.initialTs = initialTs;
    return this;
  }

  public MongoDBOplogSourceBuilder initialOrdinal(int initialOrdinal) {
    mongoDBOplogSourceConfigBean.initialOrdinal = initialOrdinal;
    return this;
  }

  public MongoDBOplogSourceBuilder addOplogOpTypeFilter(OplogOpType oplogOpType) {
    mongoDBOplogSourceConfigBean.filterOplogOpTypes.add(oplogOpType);
    return this;
  }

  public MongoDBOplogSourceBuilder filterOlogOpTypeFilter(List<OplogOpType> filterOplogTypes) {
    mongoDBOplogSourceConfigBean.filterOplogOpTypes = filterOplogTypes;
    return this;
  }

  @Override
  public MongoDBOplogSource build() {
    return new MongoDBOplogSource(config, mongoDBOplogSourceConfigBean);
  }

}
