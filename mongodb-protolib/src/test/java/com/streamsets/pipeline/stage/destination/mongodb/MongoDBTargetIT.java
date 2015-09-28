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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import de.flapdoodle.embed.mongo.MongodExecutable;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;

public class MongoDBTargetIT {
  private static final String DATABASE_NAME = "testDatabase1";
  private static final String TEST_WRITE_COLLECTION = "testWrite";
  private static final String UNIQUE_KEY_EXCEPTION_COLLECTION = "testRecordDoesNotContainUniqueKeyException";

  private static MongoCollection<Document> testWriteCollection = null;
  private static MongodExecutable mongodExecutable = null;
  private static MongoClient mongo = null;
  private static final int MONGO_PORT = 27017;

  @ClassRule
  public static GenericContainer mongoContainer = new GenericContainer("mongo:3.0").withExposedPorts(MONGO_PORT);

  @BeforeClass
  public static void setUpClass() throws Exception {
    mongo = new MongoClient(mongoContainer.getContainerIpAddress(), mongoContainer.getMappedPort(MONGO_PORT));
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);
    db.createCollection(TEST_WRITE_COLLECTION);
    db.createCollection(UNIQUE_KEY_EXCEPTION_COLLECTION);
    testWriteCollection = db.getCollection(TEST_WRITE_COLLECTION);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    mongo.close();
    if (mongodExecutable != null) {
      mongodExecutable.stop();
    }
  }

  @Test
  public void testWrite() throws StageException, IOException {

    MongoTargetConfigBean mongoTargetConfigBean = new MongoTargetConfigBean();
    mongoTargetConfigBean.mongoClientURI = "mongodb://" + mongoContainer.getContainerIpAddress() + ":" + mongoContainer.getMappedPort(MONGO_PORT);
    mongoTargetConfigBean.collection = TEST_WRITE_COLLECTION;
    mongoTargetConfigBean.database = DATABASE_NAME;
    mongoTargetConfigBean.uniqueKeyField = "/name";
    mongoTargetConfigBean.writeConcern = WriteConcernLabel.JOURNALED;

    TargetRunner targetRunner = new TargetRunner.Builder(
      MongoDBDTarget.class,
      new MongoDBTarget(mongoTargetConfigBean)
    ).build();

    targetRunner.runInit();
    List<Record> logRecords = createJsonRecords();
    targetRunner.runWrite(logRecords);

    //compare written records
    MongoCursor<Document> iterator = testWriteCollection.find().iterator();
    int count = 0;
    while(iterator.hasNext()) {
      Document next = iterator.next();
      Assert.assertEquals("NAME" + count, next.get("name"));
      Assert.assertEquals(count, next.get("lastStatusChange"));
      count++;
    }
    Assert.assertEquals(20, count);

    List<Record> toDelete = new ArrayList<>();
    //delete some records in the next batch
    for(int i = 0; i < logRecords.size(); i++) {
      Record record = logRecords.get(i);
      if(i % 2 == 0) {
        record.getHeader().setAttribute(MongoDBTarget.OPERATION_KEY, MongoDBTarget.DELETE);
      } else {
        record.getHeader().setAttribute(MongoDBTarget.OPERATION_KEY, MongoDBTarget.UPSERT);
        record.get().getValueAsMap().put("updated", Field.create(true));
      }
      toDelete.add(record);
    }
    targetRunner.runWrite(toDelete);

    // make sure records are deleted
    iterator = testWriteCollection.find().iterator();
    count = 0;
    while(iterator.hasNext()) {
      Document next = iterator.next();
      Object updated = next.get("updated");
      Assert.assertEquals(true, updated);
      count++;
    }
    Assert.assertEquals(10, count);

    targetRunner.runDestroy();
  }

  @Test
  public void testRecordDoesNotContainUniqueKeyException() throws StageException, IOException {

    MongoTargetConfigBean mongoTargetConfigBean = new MongoTargetConfigBean();
    // wrong port
    mongoTargetConfigBean.mongoClientURI = "mongodb://" + mongoContainer.getContainerIpAddress() + ":" + mongoContainer.getMappedPort(MONGO_PORT);
    mongoTargetConfigBean.collection = UNIQUE_KEY_EXCEPTION_COLLECTION;
    mongoTargetConfigBean.database = DATABASE_NAME;
    mongoTargetConfigBean.uniqueKeyField = "/randomUniqueKeyField";
    mongoTargetConfigBean.writeConcern = WriteConcernLabel.NORMAL;

    TargetRunner targetRunner = new TargetRunner.Builder(
      MongoDBDTarget.class,
      new MongoDBTarget(mongoTargetConfigBean)
    ).setOnRecordError(OnRecordError.TO_ERROR).build();

    targetRunner.runInit();
    List<Record> logRecords = createJsonRecords();

    targetRunner.runWrite(logRecords);

    List<Record> errorRecords = targetRunner.getErrorRecords();
    Assert.assertEquals(logRecords.size(), errorRecords.size());

  }

  private List<Record> createJsonRecords() throws IOException {
    List<Record> list = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Record record = RecordCreator.create();
      record.getHeader().setAttribute(MongoDBTarget.OPERATION_KEY, MongoDBTarget.UPSERT);
      Map<String, Field> map = new HashMap<>();
      map.put("name", Field.create("NAME" + i));
      map.put("lastStatusChange", Field.create(i));
      record.set(Field.create(map));
      list.add(record);
    }
    return list;
  }
}
