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

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoQueryException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.mongodb.Errors;
import com.streamsets.pipeline.stage.common.mongodb.Groups;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractMongoDBSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSource.class);

  protected final MongoSourceConfigBean configBean;

  protected ErrorRecordHandler errorRecordHandler;
  protected MongoCollection<Document> mongoCollection;
  protected MongoCursor<Document> cursor;

  private MongoClient mongoClient;

  public AbstractMongoDBSource(MongoSourceConfigBean configBean) {
    this.configBean = configBean;
    this.cursor = null;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    if (!issues.isEmpty()) {
      return issues;
    }

    configBean.mongoConfig.init(getContext(), issues, configBean.readPreference.getReadPreference(), null);

    if (issues.isEmpty()) {
      // since no issue was found in validation, the followings must not be null at this point.
      Utils.checkNotNull(configBean.mongoConfig.getMongoDatabase(), "MongoDatabase");
      mongoClient = Utils.checkNotNull(configBean.mongoConfig.getMongoClient(), "MongoClient");
      mongoCollection = Utils.checkNotNull(configBean.mongoConfig.getMongoCollection(), "MongoCollection");
      checkCursor(issues);
    }

    return issues;
  }

  @Override
  public void destroy() {
    IOUtils.closeQuietly(cursor);
    IOUtils.closeQuietly(mongoClient);
    cursor = null;
    mongoClient = null;
    super.destroy();
  }

  private void checkCursor(List<ConfigIssue> issues) {
    //According to MongoDB Oplog: https://docs.mongodb.com/manual/reference/method/cursor.batchSize/
    //We should not use batch size of 1, and in mongo db world batch size of 1 is special
    //and equal to specifying limit 1, so the below queries will simply use limit 1
    //rather than batchsize and limit both 1.
    if (configBean.isCapped) {
      try {
        mongoCollection.find().cursorType(CursorType.TailableAwait).limit(1).iterator().close();
      } catch (MongoQueryException e) {
        LOG.error("Error during Mongo Query in checkCursor: {}", e);
        issues.add(getContext().createConfigIssue(
            Groups.MONGODB.name(),
            MongoDBConfig.MONGO_CONFIG_PREFIX + "collection",
            Errors.MONGODB_04,
            configBean.mongoConfig.collection,
            e.toString()
        ));
      }
    } else {
      try {
        mongoCollection.find().cursorType(CursorType.NonTailable).limit(1).iterator().close();
      } catch (MongoQueryException e) {
        LOG.error("Error during Mongo Query in checkCursor: {}", e);
        issues.add(getContext().createConfigIssue(
            Groups.MONGODB.name(),
            MongoDBConfig.MONGO_CONFIG_PREFIX + "collection",
            Errors.MONGODB_06,
            configBean.mongoConfig.collection,
            e.toString()
        ));
      }
    }
  }
}
