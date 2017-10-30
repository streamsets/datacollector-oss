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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.common.mongodb.AuthenticationType;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.Assert.assertEquals;

public class MongoDBTargetIT {
  private static final String DATABASE_NAME = "testDatabase1";
  private static final String TEST_WRITE_COLLECTION = "testWrite";
  private static final String UNIQUE_KEY_EXCEPTION_COLLECTION = "testRecordDoesNotContainUniqueKeyException";

  private static MongoCollection<Document> testWriteCollection = null;
  private static MongoClient mongo = null;
  private static final int MONGO_PORT = 27017;

  @ClassRule
  public static GenericContainer mongoContainer = new GenericContainer("mongo:3.0").withExposedPorts(MONGO_PORT);

  @BeforeClass
  public static void setUpClass() throws Exception {
    mongo = new MongoClient(mongoContainer.getContainerIpAddress(), mongoContainer.getMappedPort(MONGO_PORT));
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    mongo.close();
  }

  @Before
  public void setUp() throws Exception {
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);
    db.createCollection(TEST_WRITE_COLLECTION);
    db.createCollection(UNIQUE_KEY_EXCEPTION_COLLECTION);
    testWriteCollection = db.getCollection(TEST_WRITE_COLLECTION);
    testWriteCollection.createIndex(Indexes.text("name"), new IndexOptions().unique(true));
  }

  @After
  public void tearDown() throws Exception {
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);
    db.getCollection(TEST_WRITE_COLLECTION).drop();
    db.getCollection(UNIQUE_KEY_EXCEPTION_COLLECTION).drop();
  }


  @Test
  public void testWrite() throws StageException, IOException {
    MongoTargetConfigBean mongoTargetConfigBean = new MongoTargetConfigBean();
    mongoTargetConfigBean.mongoConfig = new MongoDBConfig();
    mongoTargetConfigBean.mongoConfig.connectionString =
        "mongodb://" + mongoContainer.getContainerIpAddress() + ":" + mongoContainer.getMappedPort(MONGO_PORT);
    mongoTargetConfigBean.mongoConfig.collection = TEST_WRITE_COLLECTION;
    mongoTargetConfigBean.mongoConfig.database = DATABASE_NAME;
    mongoTargetConfigBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    mongoTargetConfigBean.mongoConfig.username = null;
    mongoTargetConfigBean.mongoConfig.password = null;
    mongoTargetConfigBean.uniqueKeyField = "/name";
    mongoTargetConfigBean.writeConcern = WriteConcernLabel.JOURNALED;
    mongoTargetConfigBean.isUpsert = true;

    TargetRunner targetRunner = new TargetRunner.Builder(
      MongoDBDTarget.class,
      new MongoDBTarget(mongoTargetConfigBean)
    ).build();

    targetRunner.runInit();
    List<Record> logRecords = createJsonRecords(OperationType.REPLACE_CODE);
    targetRunner.runWrite(logRecords);

    // compare written records
    MongoCursor<Document> iterator = testWriteCollection.find().iterator();
    int count = 0;
    Random rand = new Random(0);
    while(iterator.hasNext()) {
      Document next = iterator.next();
      assertEquals("NAME" + count, next.get("name"));
      assertEquals(count, next.get("lastStatusChange"));
      assertEquals(rand.nextInt(70), next.get("age"));
      count++;
    }
    assertEquals(20, count);

    List<Record> toDelete = new ArrayList<>();
    // delete some records in the next batch
    for(int i = 0; i < logRecords.size(); i++) {
      Record record = logRecords.get(i);
      if(i % 2 == 0) {
        record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.DELETE_CODE));
      } else {
        record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.REPLACE_CODE));
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
      assertEquals(true, updated);
      count++;
    }
    assertEquals(10, count);

    targetRunner.runDestroy();
  }

  @Test
  public void testUpdateWithUpsert() throws Exception {
    MongoTargetConfigBean mongoTargetConfigBean = new MongoTargetConfigBean();
    mongoTargetConfigBean.mongoConfig = new MongoDBConfig();
    mongoTargetConfigBean.mongoConfig.connectionString =
        "mongodb://" + mongoContainer.getContainerIpAddress() + ":" + mongoContainer.getMappedPort(MONGO_PORT);
    mongoTargetConfigBean.mongoConfig.collection = TEST_WRITE_COLLECTION;
    mongoTargetConfigBean.mongoConfig.database = DATABASE_NAME;
    mongoTargetConfigBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    mongoTargetConfigBean.mongoConfig.username = null;
    mongoTargetConfigBean.mongoConfig.password = null;
    mongoTargetConfigBean.uniqueKeyField = "/name";
    mongoTargetConfigBean.writeConcern = WriteConcernLabel.JOURNALED;
    mongoTargetConfigBean.isUpsert = true;

    TargetRunner targetRunner = new TargetRunner.Builder(
        MongoDBDTarget.class,
        new MongoDBTarget(mongoTargetConfigBean)
    ).build();

    targetRunner.runInit();
    List<Record> logRecords = createJsonRecords(OperationType.INSERT_CODE);
    targetRunner.runWrite(logRecords);

    List<Record> updatedRecords = new ArrayList<>();
    Record update = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("name", Field.create("NAME" + 2));
    map.put("dlnum", Field.create("12345-55"));
    update.set(Field.create(map));
    update.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.UPDATE_CODE));
    updatedRecords.add(update);

    targetRunner.runWrite(updatedRecords);

    for (Document next : testWriteCollection.find(eq("name", "NAME" + 2))) {
      assertEquals("12345-55", next.get("dlnum"));
      assertEquals(2, next.get("lastStatusChange"));
    }
  }

  @Test
  public void testUpsertIsInvalidOperation() throws Exception {
    MongoTargetConfigBean mongoTargetConfigBean = new MongoTargetConfigBean();
    mongoTargetConfigBean.mongoConfig = new MongoDBConfig();
    mongoTargetConfigBean.mongoConfig.connectionString =
        "mongodb://" + mongoContainer.getContainerIpAddress() + ":" + mongoContainer.getMappedPort(MONGO_PORT);
    mongoTargetConfigBean.mongoConfig.collection = TEST_WRITE_COLLECTION;
    mongoTargetConfigBean.mongoConfig.database = DATABASE_NAME;
    mongoTargetConfigBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    mongoTargetConfigBean.mongoConfig.username = null;
    mongoTargetConfigBean.mongoConfig.password = null;
    mongoTargetConfigBean.uniqueKeyField = "/name";
    mongoTargetConfigBean.writeConcern = WriteConcernLabel.JOURNALED;
    mongoTargetConfigBean.isUpsert = false;

    TargetRunner targetRunner = new TargetRunner.Builder(MongoDBDTarget.class, new MongoDBTarget(mongoTargetConfigBean))
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    targetRunner.runInit();
    List<Record> logRecords = createJsonRecords(OperationType.UPSERT_CODE);
    targetRunner.runWrite(logRecords);

    List<Record> errorRecords = targetRunner.getErrorRecords();
    assertEquals(logRecords.size(), errorRecords.size());
  }

  @Test
  public void testRecordDoesNotContainUniqueKeyException() throws StageException, IOException {
    MongoTargetConfigBean mongoTargetConfigBean = new MongoTargetConfigBean();
    mongoTargetConfigBean.mongoConfig = new MongoDBConfig();
    // wrong port
    mongoTargetConfigBean.mongoConfig.connectionString =
        "mongodb://" + mongoContainer.getContainerIpAddress() + ":" + mongoContainer.getMappedPort(MONGO_PORT);
    mongoTargetConfigBean.mongoConfig.collection = UNIQUE_KEY_EXCEPTION_COLLECTION;
    mongoTargetConfigBean.mongoConfig.database = DATABASE_NAME;
    mongoTargetConfigBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    mongoTargetConfigBean.mongoConfig.username = null;
    mongoTargetConfigBean.mongoConfig.password = null;
    mongoTargetConfigBean.uniqueKeyField = "/randomUniqueKeyField";
    mongoTargetConfigBean.writeConcern = WriteConcernLabel.NORMAL;

    TargetRunner targetRunner = new TargetRunner.Builder(
      MongoDBDTarget.class,
      new MongoDBTarget(mongoTargetConfigBean)
    ).setOnRecordError(OnRecordError.TO_ERROR).build();

    targetRunner.runInit();
    List<Record> logRecords = createJsonRecords(OperationType.REPLACE_CODE);

    targetRunner.runWrite(logRecords);

    List<Record> errorRecords = targetRunner.getErrorRecords();
    assertEquals(logRecords.size(), errorRecords.size());

  }

  private List<Record> createJsonRecords(int operationCode) throws IOException {
    final String operation = String.valueOf(operationCode);
    final Random rand = new Random(0);
    List<Record> list = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Record record = RecordCreator.create();
      record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, operation);
      Map<String, Field> map = new HashMap<>();
      map.put("name", Field.create("NAME" + i));
      map.put("age", Field.create(rand.nextInt(70)));
      map.put("lastStatusChange", Field.create(i));
      record.set(Field.create(map));
      list.add(record);
    }
    return list;
  }
}
