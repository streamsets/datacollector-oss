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
package com.streamsets.pipeline.stage.destination.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.mongodb.Errors;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;

public class MongoDBTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDBTarget.class);

  public static final int DEFAULT_CAPACITY = 1024;

  private final MongoTargetConfigBean mongoTargetConfigBean;
  private MongoClient mongoClient;
  private MongoCollection<Document> mongoCollection;
  private ErrorRecordHandler errorRecordHandler;
  private DataGeneratorFactory generatorFactory;

  public MongoDBTarget(MongoTargetConfigBean mongoTargetConfigBean) {
    this.mongoTargetConfigBean = mongoTargetConfigBean;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    mongoTargetConfigBean.mongoConfig.init(
        getContext(),
        issues,
        null,
        mongoTargetConfigBean.writeConcern.getWriteConcern()
    );
    if (!issues.isEmpty()) {
      return issues;
    }

    // since no issue was found in validation, the followings must not be null at this point.
    Utils.checkNotNull(mongoTargetConfigBean.mongoConfig.getMongoDatabase(), "MongoDatabase");
    mongoClient = Utils.checkNotNull(mongoTargetConfigBean.mongoConfig.getMongoClient(), "MongoClient");
    mongoCollection = Utils.checkNotNull(mongoTargetConfigBean.mongoConfig.getMongoCollection(), "MongoCollection");

    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(
        getContext(),
        DataFormat.JSON.getGeneratorFormat()
    );
    builder.setCharset(StandardCharsets.UTF_8);
    builder.setMode(Mode.MULTIPLE_OBJECTS);
    generatorFactory = builder.build();

    return issues;
  }

  @Override
  public void destroy() {
    IOUtils.closeQuietly(mongoClient);
    super.destroy();
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    List<WriteModel<Document>> documentList = new ArrayList<>();
    List<Record> recordList = new ArrayList<>();
    while (records.hasNext()) {
      Record record = records.next();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(DEFAULT_CAPACITY);
        DataGenerator generator = generatorFactory.getGenerator(baos);
        generator.write(record);
        generator.close();
        Document document = Document.parse(new String(baos.toByteArray()));

        // create a write model based on record header
        if (isNullOrEmpty(record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE))) {
          LOG.error(Errors.MONGODB_15.getMessage(), record.getHeader().getSourceId());
          throw new OnRecordErrorException(Errors.MONGODB_15, record.getHeader().getSourceId());
        }

        String operationCode = record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE);
        String operation;
        if (operationCode != null) {
          operation = OperationType.getLabelFromStringCode(operationCode);
        } else {
          operation = record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE);
        }
        if (operation == null) {
          throw new StageException(Errors.MONGODB_15, record.getHeader().getSourceId());
        }
        switch (operation.toUpperCase()) {
          case "INSERT":
            documentList.add(new InsertOneModel<>(document));
            recordList.add(record);
            break;
          case "REPLACE":
            validateUniqueKey(operation, record);
            recordList.add(record);
            documentList.add(
                new ReplaceOneModel<>(
                    new Document(
                        removeLeadingSlash(mongoTargetConfigBean.uniqueKeyField),
                        record.get(mongoTargetConfigBean.uniqueKeyField).getValueAsString()
                    ),
                    document,
                    new UpdateOptions().upsert(mongoTargetConfigBean.isUpsert)
                )
            );
            break;
          case "UPDATE":
            validateUniqueKey(operation, record);
            recordList.add(record);
            documentList.add(
                new UpdateOneModel<>(
                    new Document(
                        removeLeadingSlash(mongoTargetConfigBean.uniqueKeyField),
                        record.get(mongoTargetConfigBean.uniqueKeyField).getValueAsString()
                    ),
                    new Document("$set", document),
                    new UpdateOptions().upsert(mongoTargetConfigBean.isUpsert)
                )
            );
            break;
          case "DELETE":
            recordList.add(record);
            documentList.add(new DeleteOneModel<>(document));
            break;
          default:
            LOG.error(Errors.MONGODB_14.getMessage(), operation, record.getHeader().getSourceId());
            throw new StageException(Errors.MONGODB_14, operation, record.getHeader().getSourceId());
        }
      } catch (IOException | StageException | NumberFormatException e) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                Errors.MONGODB_13,
                e.toString(),
                e
            )
        );
      }
    }

    if (!documentList.isEmpty()) {
      try {
        BulkWriteResult bulkWriteResult = mongoCollection.bulkWrite(documentList);
        if (bulkWriteResult.wasAcknowledged()) {
          LOG.trace(
              "Wrote batch with {} inserts, {} updates and {} deletes",
              bulkWriteResult.getInsertedCount(),
              bulkWriteResult.getModifiedCount(),
              bulkWriteResult.getDeletedCount()
          );
        }
      } catch (MongoException e) {
        for (Record record : recordList) {
          errorRecordHandler.onError(
              new OnRecordErrorException(
                  record,
                  Errors.MONGODB_17,
                  e.toString(),
                  e
              )
          );
        }
      }
    }
  }

  private void validateUniqueKey(String operation, Record record) throws OnRecordErrorException {
    if(mongoTargetConfigBean.uniqueKeyField == null || mongoTargetConfigBean.uniqueKeyField.isEmpty()) {
      LOG.error(
          Errors.MONGODB_18.getMessage(),
          operation
      );
      throw new OnRecordErrorException(
          Errors.MONGODB_18,
          operation
      );
    }

    if (!record.has(mongoTargetConfigBean.uniqueKeyField)) {
      LOG.error(
        Errors.MONGODB_16.getMessage(),
        record.getHeader().getSourceId(),
        mongoTargetConfigBean.uniqueKeyField
      );
      throw new OnRecordErrorException(
        Errors.MONGODB_16,
        record.getHeader().getSourceId(),
        mongoTargetConfigBean.uniqueKeyField
      );
    }
  }

  private String removeLeadingSlash(String uniqueKeyField) {
    if(uniqueKeyField.startsWith("/")) {
      return uniqueKeyField.substring(1);
    }
    return uniqueKeyField;
  }
}
