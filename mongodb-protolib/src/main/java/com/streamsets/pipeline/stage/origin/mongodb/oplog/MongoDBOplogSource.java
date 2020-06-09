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

import com.google.common.base.Joiner;
import com.mongodb.CursorType;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.mongodb.Errors;
import com.streamsets.pipeline.stage.common.mongodb.Groups;
import com.streamsets.pipeline.stage.origin.mongodb.AbstractMongoDBSource;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBUtil;
import com.streamsets.pipeline.stage.origin.mongodb.MongoSourceConfigBean;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MongoDBOplogSource extends AbstractMongoDBSource {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDBOplogSource.class);
  private static final String OPLOG_COLLECTION_PREFIX = "oplog.";
  private static final Joiner COMMA_JOINER = Joiner.on(",");
  private static final String OFFSET_SEPARATOR = "::";

  static final String TIMESTAMP_FIELD = "ts";
  static final String OP_TYPE_FIELD = "op";
  static final String OP_FIELD = "o";
  static final String NS_FIELD = "ns";
  static final String OP_LONG_HASH_FIELD = "h";
  static final String VERSION_FIELD = "v";
  static final List<String> MANDATORY_FIELDS_IN_RECORD = Arrays.asList(TIMESTAMP_FIELD, NS_FIELD, OP_TYPE_FIELD, OP_FIELD, OP_LONG_HASH_FIELD, VERSION_FIELD);

  private final MongoDBOplogSourceConfigBean mongoDBOplogSourceConfigBean;

  private int lastOffsetTsSeconds;
  private int lastOffsetTsOrdinal;

  private boolean checkBatchSize = true;

  public MongoDBOplogSource(MongoSourceConfigBean configBean, MongoDBOplogSourceConfigBean mongoDBOplogSourceConfigBean) {
    super(configBean);
    this.mongoDBOplogSourceConfigBean = mongoDBOplogSourceConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (issues.isEmpty()) {
      extraInit(getContext(), issues);
    }
    if (issues.isEmpty()) {
      lastOffsetTsSeconds = mongoDBOplogSourceConfigBean.initialTs;
      lastOffsetTsOrdinal = mongoDBOplogSourceConfigBean.initialOrdinal;
    }
    return issues;
  }

  //@VisibleForTesting
  void extraInit(Stage.Context context, List<ConfigIssue> issues) {
    if (!configBean.mongoConfig.collection.startsWith(OPLOG_COLLECTION_PREFIX)) {
      issues.add(
          context.createConfigIssue(
              Groups.MONGODB.name(),
              "configBean.mongoConfig.collection",
              Errors.MONGODB_33,
              configBean.mongoConfig.collection)
      );
    }
    mongoDBOplogSourceConfigBean.init(context, issues);
  }
  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int batchSize = Math.min(maxBatchSize, configBean.batchSize);
    if (!getContext().isPreview() && checkBatchSize && configBean.batchSize > maxBatchSize) {
      getContext().reportError(Errors.MONGODB_43, maxBatchSize);
      checkBatchSize = false;
    }

    long batchWaitTime = System.currentTimeMillis() + (configBean.maxBatchWaitTime * 1000);
    int numOfRecordsProduced = 0;
    initStateIfNeeded(lastSourceOffset, batchSize);
    while (numOfRecordsProduced < batchSize) {
      try {
        Record record = getOplogRecord();
        if (record != null) {
          batchMaker.addRecord(record);
          numOfRecordsProduced++;
        } else if (breakOrWaitIfNeeded(batchWaitTime - System.currentTimeMillis())){
          break;
        }
      } catch (IOException | MongoException | IllegalArgumentException e) {
        LOG.error("Error while getting Oplog Record", e);
        errorRecordHandler.onError(Errors.MONGODB_10, e.toString(), e);
      }
    }
    return createOffset();
  }

  private boolean breakOrWaitIfNeeded(long remainingWaitTime) {
    //Wait the remaining time if there is no record
    //and try for last time before to read records and then return whatever was read
    if (remainingWaitTime > 0) {
      ThreadUtil.sleep(remainingWaitTime);
      //Don't break and try reading now
      return false;
    }
    //break
    return true;
  }

  private boolean shouldInitOffset(String lastOffset) {
    return !StringUtils.isEmpty(lastOffset);
  }

  private void initStateIfNeeded(String lastOffset, int batchSize) {
    if (shouldInitOffset(lastOffset)) {
      String[] offsetSplit = lastOffset.split(OFFSET_SEPARATOR);
      if (offsetSplit.length != 2) {
        throw new IllegalArgumentException(new StageException(Errors.MONGODB_31, lastOffset, "Does not confirm to offset notation"));
      }
      try {
        lastOffsetTsSeconds = Integer.parseInt(offsetSplit[0]);
        lastOffsetTsOrdinal = Integer.parseInt(offsetSplit[1]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(new StageException(Errors.MONGODB_31, lastOffset, e));
      }
    }
    if (cursor == null) {
      prepareCursor(lastOffsetTsSeconds, lastOffsetTsOrdinal, mongoDBOplogSourceConfigBean.filterOplogOpTypes, batchSize);
    }
  }

  private void prepareCursor(int timestampSeconds, int ordinal, List<OplogOpType> filterOplogTypes, int batchSize) {
    LOG.debug("Getting new cursor with offset - TimeStampInSeconds:'{}', Ordinal : '{}' and Batch Size : '{}'",timestampSeconds, ordinal, batchSize);
    FindIterable<Document> mongoCursorIterable = mongoCollection
        .find()
        //As the collection is a capped collection we use Tailable cursor which will return results in natural order in this case
        //based on ts timestamp field.
        //Tailable Await does not return and blocks, so we are using tailable.
        .cursorType(CursorType.Tailable)
        .batchSize(batchSize);

    List<Bson> andFilters = new ArrayList<>();
    //Only filter if we already have saved/initial offset specified or else both time_t and ordinal will not be -1.
    if (timestampSeconds > 0 && ordinal >= 0) {
      andFilters.add(Filters.gt(TIMESTAMP_FIELD, new BsonTimestamp(timestampSeconds, ordinal)));
    }

    if (!filterOplogTypes.isEmpty()) {
      List<Bson> oplogOptypeFilters = new ArrayList<>();
      Set<OplogOpType> oplogOpTypesSet = new HashSet<>();
      for (OplogOpType filterOplogopType : filterOplogTypes) {
        if (oplogOpTypesSet.add(filterOplogopType)) {
          oplogOptypeFilters.add(Filters.eq(OP_TYPE_FIELD, filterOplogopType.getOp()));
        }
      }
      //Add an or filter for filtered Or Types
      andFilters.add(Filters.or(oplogOptypeFilters));
    }
    //Finally and timestamp with oplog filters
    if (!andFilters.isEmpty()) {
      mongoCursorIterable = mongoCursorIterable.filter(Filters.and(andFilters));
    }
    cursor = mongoCursorIterable.iterator();
  }

  private void validateOpLogDocument(Document document) throws IOException {
    List<String> missingFields = new ArrayList<>();
    for (String mandatoryField : MANDATORY_FIELDS_IN_RECORD) {
      if (!document.containsKey(mandatoryField)) {
        missingFields.add(mandatoryField);
      }
    }
    if (!missingFields.isEmpty()) {
      throw new IllegalArgumentException(new StageException(Errors.MONGODB_30, COMMA_JOINER.join(missingFields)));
    }
  }

  private String createOffset() {
    if (lastOffsetTsSeconds != -1 && lastOffsetTsOrdinal != -1) {
      return String.valueOf(lastOffsetTsSeconds) + OFFSET_SEPARATOR + String.valueOf(lastOffsetTsOrdinal);
    }
    return "";
  }

  //@VisibleForTesting
  static void populateGenericOperationTypeInHeader(Record record, String opType) {
    OplogOpType oplogOpType = OplogOpType.getOplogTypeFromOpString(opType);
    if (oplogOpType == null) {
      throw new IllegalArgumentException(Utils.format("Unsupported Op Log Op type : {}", opType));
    }
    //All operation types in OperationType are positive, so using -1 to indicate no matching operation type
    int operationType;
    switch (oplogOpType) {
      case INSERT:
        operationType = OperationType.INSERT_CODE;
        break;
      case UPDATE:
        operationType = OperationType.UPDATE_CODE;
        break;
      case DELETE:
        operationType = OperationType.DELETE_CODE;
        break;
      //These specific to Mongo DB so not handling this for generic cases.
      case CMD:
      case DB:
      case NOOP:
        operationType = OperationType.UNSUPPORTED_CODE;
        break;
      default: throw new IllegalArgumentException(Utils.format("Unsupported Op Log Op type : {}", opType));
    }
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(operationType));
  }

  private Record getOplogRecord() throws IOException {
    Document doc = cursor.tryNext();
    if (doc != null) {
      validateOpLogDocument(doc);
      BsonTimestamp timestamp = (BsonTimestamp) doc.get(TIMESTAMP_FIELD);
      lastOffsetTsSeconds = timestamp.getTime();
      lastOffsetTsOrdinal = timestamp.getInc();

      //This does not seem to be always increasing, but is unique,
      // we are not using it for offset but using it for source record id
      Long opId = doc.getLong(OP_LONG_HASH_FIELD);

      Record record = getContext().createRecord(
              MongoDBUtil.getSourceRecordId(
              configBean.mongoConfig.connectionString,
              configBean.mongoConfig.database,
              configBean.mongoConfig.collection,
              String.valueOf(opId) + "::" + createOffset()
          )
      );
      String ns = doc.getString(NS_FIELD);
      String opType = doc.getString(OP_TYPE_FIELD);
      record.getHeader().setAttribute(NS_FIELD, ns);
      record.getHeader().setAttribute(OP_TYPE_FIELD, opType);
      //Populate Generic operation type
      populateGenericOperationTypeInHeader(record, opType);
      record.set(Field.create(MongoDBUtil.createFieldFromDocument(doc)));
      return record;
    } else {
      LOG.trace("Document from Cursor is null, No More Records");
    }
    return null;
  }
}
