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
package com.streamsets.pipeline.stage.destination.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.stage.common.mongodb.Errors;
import com.streamsets.pipeline.stage.destination.lib.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.lib.ErrorRecordHandler;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MongoDBTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBTarget.class);
  private ErrorRecordHandler errorRecordHandler;

  public static final String OPERATION_KEY = "SDC.MONGODB.OPERATION";
  public static final String INSERT = "INSERT";
  public static final String UPSERT = "UPSERT";
  public static final String DELETE = "DELETE";
  public static final int DEFAULT_CAPACITY = 1024;

  private final MongoTargetConfigBean mongoTargetConfigBean;
  private MongoCollection<Document> coll;
  private DataGeneratorFactory generatorFactory;
  private MongoClient mongoClient;

  public MongoDBTarget(MongoTargetConfigBean mongoTargetConfigBean) {
    this.mongoTargetConfigBean = mongoTargetConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    MongoClientURI connectionString = new MongoClientURI(mongoTargetConfigBean.mongoClientURI);
    mongoClient = new MongoClient(connectionString);
    MongoDatabase db = mongoClient.getDatabase(mongoTargetConfigBean.database);
    coll = db.getCollection(mongoTargetConfigBean.collection).
        withWriteConcern(mongoTargetConfigBean.writeConcern.getWriteConcern());

    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(
        getContext(),
        DataFormat.JSON.getGeneratorFormat()
    );
    builder.setCharset(StandardCharsets.UTF_8);

    builder.setMode(JsonMode.MULTIPLE_OBJECTS);
    generatorFactory = builder.build();

    return issues;
  }

  @Override
  public void destroy() {
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
    }
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

        //create a write model based on record header
        if (!record.getHeader().getAttributeNames().contains(OPERATION_KEY)) {
          LOG.error(
              Errors.MONGODB_15.getMessage(),
              record.getHeader().getSourceId()
          );
          throw new OnRecordErrorException(
              Errors.MONGODB_15,
              record.getHeader().getSourceId()
          );
        }

        String operation = record.getHeader().getAttribute(OPERATION_KEY);
        switch (operation) {
          case INSERT:
            documentList.add(new InsertOneModel<>(document));
            recordList.add(record);
            break;
          case UPSERT:
            validateUniqueKey(operation, record);
            recordList.add(record);
            documentList.add(
                new ReplaceOneModel<>(
                    new Document(
                        removeLeadingSlash(mongoTargetConfigBean.uniqueKeyField),
                        record.get(mongoTargetConfigBean.uniqueKeyField).getValueAsString()
                    ),
                    document,
                    new UpdateOptions().upsert(true)
                )
            );
            break;
          case DELETE:
            recordList.add(record);
            documentList.add(new DeleteOneModel<Document>(document));
            break;
          default:
            LOG.error(
                Errors.MONGODB_14.getMessage(),
                operation,
                record.getHeader().getSourceId()
            );
            throw new StageException(
                Errors.MONGODB_14,
                operation,
                record.getHeader().getSourceId()
            );
        }
      } catch (IOException | StageException e) {
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
        BulkWriteResult bulkWriteResult = coll.bulkWrite(documentList);
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
