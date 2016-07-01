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
package com.streamsets.pipeline.stage.origin.mongodb;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoQueryException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.mongodb.Errors;
import com.streamsets.pipeline.stage.common.mongodb.Groups;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoDBSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSource.class);
  private static final String _ID = "_id";

  private final MongoSourceConfigBean configBean;

  private ObjectId initialObjectId;
  private ErrorRecordHandler errorRecordHandler;
  private MongoClient mongoClient;
  private MongoCollection<Document> mongoCollection;
  private MongoCursor<Document> cursor;

  public MongoDBSource(MongoSourceConfigBean configBean) {
    this.configBean = configBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    try {
      initialObjectId = new ObjectId(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(configBean.initialOffset));
    } catch (ParseException e) {
      issues.add(
          getContext().createConfigIssue(
              Groups.MONGODB.name(),
              MongoDBConfig.CONFIG_PREFIX + "initialOffset",
              Errors.MONGODB_05,
              configBean.initialOffset
          )
      );
    }

    configBean.mongoConfig.init(getContext(), issues, configBean.readPreference.getReadPreference(), null);
    if (!issues.isEmpty()) {
      return issues;
    }

    // since no issue was found in validation, the followings must not be null at this point.
    Utils.checkNotNull(configBean.mongoConfig.getMongoDatabase(), "MongoDatabase");
    mongoClient = Utils.checkNotNull(configBean.mongoConfig.getMongoClient(), "MongoClient");
    mongoCollection = Utils.checkNotNull(configBean.mongoConfig.getMongoCollection(), "MongoCollection");

    checkCursor(issues);

    return issues;
  }

  @Override
  public void destroy() {
    IOUtils.closeQuietly(cursor);
    IOUtils.closeQuietly(mongoClient);
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    // do not return null in the case where the table is empty on startup
    lastSourceOffset = lastSourceOffset == null ? "" : lastSourceOffset;
    String nextSourceOffset = lastSourceOffset;
    int numRecords = 0;

    prepareCursor(maxBatchSize, configBean.offsetField, lastSourceOffset);
    long batchWaitTime = System.currentTimeMillis() + (configBean.maxBatchWaitTime * 1000);

    try {
      while (numRecords < Math.min(configBean.batchSize, maxBatchSize) && System.currentTimeMillis() < batchWaitTime) {
        LOG.trace("Trying to get next doc from cursor");
        Document doc = cursor.tryNext();
        if (null == doc) {
          LOG.trace("Doc was null");
          if (!configBean.isCapped) {
            LOG.trace("Collection is not capped.");
            // If this is not a capped collection, then this means we've reached the end of the data.
            // and should get a new cursor.
            LOG.trace("Closing cursor.");
            cursor.close();
            cursor = null;
            // Wait the remaining time we have for this batch before trying again.
            long waitTime = Math.max(0, batchWaitTime - System.currentTimeMillis());
            LOG.trace("Sleeping for: {}", waitTime);
            ThreadUtil.sleep(waitTime);
            return nextSourceOffset;
          }
          continue;
        }

        Set<Map.Entry<String, Object>> entrySet = doc.entrySet();
        Map<String, Field> fields = new HashMap<>(entrySet.size());

        try {
          for (Map.Entry<String, Object> entry : entrySet) {
            Field value;
            if (entry.getValue() instanceof ObjectId) {
              String objectId = entry.getValue().toString();
              value = JsonUtil.jsonToField(objectId);
            } else {
              value = JsonUtil.jsonToField(entry.getValue());
            }
            fields.put(entry.getKey(), value);
          }
        } catch (IOException e) {
          errorRecordHandler.onError(Errors.MONGODB_10, e.toString(), e);
          continue;
        }

        if (!doc.containsKey(_ID)) {
          errorRecordHandler.onError(Errors.MONGODB_11, configBean.offsetField, doc.toString());
          continue;
        }
        nextSourceOffset = doc.getObjectId(configBean.offsetField).toHexString();

        final String recordContext = configBean.mongoConfig.connectionString + "::" +
            configBean.mongoConfig.database + "::" + configBean.mongoConfig.collection + "::" +
            nextSourceOffset;

        Record record = getContext().createRecord(recordContext);
        record.set(Field.create(fields));
        batchMaker.addRecord(record);
        ++numRecords;
      }
    } catch (MongoClientException e) {
      throw new StageException(Errors.MONGODB_12, e.toString(), e);
    }
    return nextSourceOffset;
  }

  private void prepareCursor(int maxBatchSize, String offsetField, String lastSourceOffset) {
    ObjectId offset;
    if (null == cursor) {
      if (null == lastSourceOffset || lastSourceOffset.isEmpty()) {
        offset = initialObjectId;
      } else {
        offset = new ObjectId(lastSourceOffset);
      }
      LOG.debug("Getting new cursor with params: {} {} {}", maxBatchSize, offsetField, offset);
      if (configBean.isCapped) {
        cursor = mongoCollection
            .find()
            .filter(Filters.gt(offsetField, offset))
            .cursorType(CursorType.TailableAwait)
            .batchSize(maxBatchSize)
            .iterator();
      } else {
        cursor = mongoCollection
            .find()
            .filter(Filters.gt(offsetField, offset))
            .sort(Sorts.ascending(offsetField))
            .cursorType(CursorType.NonTailable)
            .batchSize(maxBatchSize)
            .iterator();
      }
    }
  }

  private void checkCursor(List<ConfigIssue> issues) {
    if (configBean.isCapped) {
      try {
        mongoCollection.find().cursorType(CursorType.TailableAwait).batchSize(1).limit(1).iterator().close();
      } catch (MongoQueryException e) {
        issues.add(getContext().createConfigIssue(
            Groups.MONGODB.name(),
            MongoDBConfig.MONGO_CONFIG_PREFIX + "collection",
            Errors.MONGODB_04,
            configBean.mongoConfig.database,
            e.toString()
        ));
      }
    } else {
      try {
        mongoCollection.find().cursorType(CursorType.NonTailable).batchSize(1).limit(1).iterator().close();
      } catch (MongoQueryException e) {
        issues.add(getContext().createConfigIssue(
            Groups.MONGODB.name(),
            MongoDBConfig.MONGO_CONFIG_PREFIX + "collection",
            Errors.MONGODB_06,
            configBean.mongoConfig.database,
            e.toString()
        ));
      }
    }
  }
}
