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
import com.mongodb.MongoClientException;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.event.CommonEvents;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.mongodb.Errors;
import com.streamsets.pipeline.stage.common.mongodb.Groups;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MongoDBSource extends AbstractMongoDBSource {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSource.class);
  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

  private ObjectId initialObjectId;
  private String initialId; // Used only when Offset Field is String type
  private Date initialDate; // MongoDB does not support LocalDateTime yet
  private boolean EOSreached = false; //end of stream reached
  private long recordsSinceLastNMREvent = 0;
  private long errorRecordsSinceLastNMREvent = 0;
  private final SimpleDateFormat dateFormatter = new SimpleDateFormat(TIMESTAMP_FORMAT);

  public MongoDBSource(MongoSourceConfigBean configBean) {
    super(configBean);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (configBean.offsetType == OffsetFieldType.STRING) {
      initialId = configBean.initialOffset; // Ok to be empty
    } else {
      // Initial offset is required if offset type is ObjectId or Date.
      if (configBean.initialOffset == null || configBean.initialOffset.isEmpty()) {
        issues.add(
            getContext().createConfigIssue(
                Groups.MONGODB.name(),
                MongoDBConfig.CONFIG_PREFIX + "initialOffset",
                Errors.MONGODB_19,
                configBean.initialOffset
            )
        );
        return issues;
      }
      try {
        initialObjectId = new ObjectId(new SimpleDateFormat(TIMESTAMP_FORMAT).parse(configBean.initialOffset));
        initialDate = dateFormatter.parse(configBean.initialOffset);
      } catch (ParseException e) {
        issues.add(
            getContext().createConfigIssue(
                Groups.MONGODB.name(),
                MongoDBConfig.CONFIG_PREFIX + "initialOffset",
                Errors.MONGODB_05,
                configBean.initialOffset,
                configBean.offsetType.getLabel()
            )
        );
      }
    }
    return issues;
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
            conditionallyGenerateNoMoreDataEvent();
            return nextSourceOffset;
          }
          continue;
        }

        // validate the date type of offset field is ObjectId or Date
        Object offsetFieldObject = doc.get(configBean.offsetField);
        if (offsetFieldObject == null
            || (configBean.offsetType == OffsetFieldType.OBJECTID && !(offsetFieldObject instanceof ObjectId))
            || (configBean.offsetType == OffsetFieldType.STRING && !(offsetFieldObject instanceof String))
            || (configBean.offsetType == OffsetFieldType.DATE && !(offsetFieldObject instanceof Date))) {
          LOG.debug(Errors.MONGODB_05.getMessage(), doc.toString(), configBean.offsetType.getLabel());
          errorRecordHandler.onError(Errors.MONGODB_05, doc, configBean.offsetType.getLabel());
          ++errorRecordsSinceLastNMREvent;
          continue;
        }

        Map<String, Field> fields;
        try {
          fields = MongoDBSourceUtil.createFieldFromDocument(doc);
        } catch (IOException e) {
          errorRecordHandler.onError(Errors.MONGODB_10, e.toString(), e);
          ++errorRecordsSinceLastNMREvent;
          continue;
        }

        // get the offsetField
        nextSourceOffset = getNextSourceOffset(doc);

        final String recordContext =
            MongoDBSourceUtil.getSourceRecordId(
                configBean.mongoConfig.connectionString,
                configBean.mongoConfig.database,
                configBean.mongoConfig.collection,
                nextSourceOffset
            );

        Record record = getContext().createRecord(recordContext);
        record.set(Field.create(fields));
        batchMaker.addRecord(record);
        ++numRecords;
        ++recordsSinceLastNMREvent;
      }
    } catch (MongoClientException e) {
      throw new StageException(Errors.MONGODB_12, e.toString(), e);
    }

    // if 2 cursors in a row have no data, we hit the end of the data stream
    if(EOSreached) {
      conditionallyGenerateNoMoreDataEvent();
    }
    EOSreached = nextSourceOffset.equals(lastSourceOffset);

    return nextSourceOffset;
  }

  private String getNextSourceOffset(Document doc) throws StageException {
    String[] keys = configBean.offsetField.split("\\.");
    return parseSourceOffset(doc, keys, 0);
  }

  private String parseSourceOffset(Document doc, String[] keys, int i) throws StageException {
    if (keys.length-1 == i) {
      if (configBean.offsetType == OffsetFieldType.STRING) {
        return doc.get(keys[i]).toString();
      }
      else if(configBean.offsetType == OffsetFieldType.OBJECTID) {
        return doc.getObjectId(keys[i]).toHexString();
      }
      else if(configBean.offsetType == OffsetFieldType.DATE){
        return dateFormatter.format(doc.getDate(keys[i]));
      }
      else {
        throw new StageException(Errors.MONGODB_20, configBean.offsetType);
      }
    }

    if (!doc.containsKey(keys[i])) {
      errorRecordHandler.onError(Errors.MONGODB_11, configBean.offsetField, doc.toString());
      ++errorRecordsSinceLastNMREvent;
    }

    return parseSourceOffset((Document)doc.get(keys[i]), keys, i+1);
  }

  private void prepareCursor(int maxBatchSize, String offsetField, String lastSourceOffset) throws StageException {
    String stringOffset = "";
    ObjectId objectIdOffset = null;
    Date dateOffset = null;
    if (null == cursor) {
      if (null == lastSourceOffset || lastSourceOffset.isEmpty()) {
        objectIdOffset = initialObjectId;
        stringOffset = initialId;
        dateOffset = initialDate;
      } else {
        if (configBean.offsetType == OffsetFieldType.STRING)
          stringOffset = lastSourceOffset;
        else if (configBean.offsetType == OffsetFieldType.OBJECTID)
          objectIdOffset = new ObjectId(lastSourceOffset);
        else if (configBean.offsetType == OffsetFieldType.DATE) {
          try {
            dateOffset = dateFormatter.parse(lastSourceOffset);
          } catch (ParseException ex) {
            throw new StageException(Errors.MONGODB_21, lastSourceOffset);
          }
        }
      }
      LOG.debug("Getting new cursor with params: {} {} {}",
          maxBatchSize,
          offsetField,
          configBean.offsetType == OffsetFieldType.STRING ? stringOffset : configBean.offsetType == OffsetFieldType.OBJECTID ? objectIdOffset : dateOffset);

      if (configBean.isCapped) {
        cursor = mongoCollection
            .find()
            .filter(Filters.gt(
                offsetField,
                configBean.offsetType ==  OffsetFieldType.STRING ? stringOffset : configBean.offsetType == OffsetFieldType.OBJECTID ? objectIdOffset : dateOffset
            ))
            .cursorType(CursorType.TailableAwait)
            .batchSize(maxBatchSize)
            .iterator();
      } else {
        cursor = mongoCollection
            .find()
            .filter(Filters.gt(
                offsetField,
                configBean.offsetType ==  OffsetFieldType.STRING ? stringOffset : configBean.offsetType == OffsetFieldType.OBJECTID ? objectIdOffset : dateOffset
            ))
            .sort(Sorts.ascending(offsetField))
            .cursorType(CursorType.NonTailable)
            .batchSize(maxBatchSize)
            .iterator();
      }
    }
  }

  private void conditionallyGenerateNoMoreDataEvent() {
    if(recordsSinceLastNMREvent != 0 || errorRecordsSinceLastNMREvent != 0) {
      CommonEvents.NO_MORE_DATA.create(getContext())
          .with("record-count", recordsSinceLastNMREvent)
          .with("error-count", errorRecordsSinceLastNMREvent)
          .createAndSend();
      recordsSinceLastNMREvent = 0;
      errorRecordsSinceLastNMREvent = 0;
    }
  }
}
