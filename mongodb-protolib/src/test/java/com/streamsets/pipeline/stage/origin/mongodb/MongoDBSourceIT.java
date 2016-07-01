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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.mongodb.AuthenticationType;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@Ignore
public class MongoDBSourceIT {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceIT.class);
  private static final String DATABASE_NAME = "test";
  private static final String CAPPED_COLLECTION = "capped";
  private static final String COLLECTION = "uncapped";
  private static final String UUID_COLLECTION = "uuid";
  private static final int TEST_COLLECTION_SIZE = 4;
  private static final int ONE_MB = 1000 * 1000;

  private static final List<Document> documents = new ArrayList<>(TEST_COLLECTION_SIZE);
  private static final UUID uuidValue = UUID.randomUUID();
  private static final int MONGO_PORT = 27017;

  @ClassRule
  public static GenericContainer mongoContainer = new GenericContainer("mongo:3.0").withExposedPorts(MONGO_PORT);

  private static int mongoContainerMappedPort = 0;
  private static String mongoContainerIp = null;

  @BeforeClass
  public static void setUpClass() throws Exception {
    for (int i = 0; i < TEST_COLLECTION_SIZE; i++) {
      documents.add(new Document("value", "document " + i));
    }
    mongoContainerMappedPort = mongoContainer.getMappedPort(MONGO_PORT);
    mongoContainerIp = mongoContainer.getContainerIpAddress();

    MongoClient mongo = new MongoClient(mongoContainerIp, mongoContainerMappedPort);
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);
    db.createCollection(CAPPED_COLLECTION, new CreateCollectionOptions().capped(true).sizeInBytes(ONE_MB));
    db.createCollection(COLLECTION);
    db.createCollection(UUID_COLLECTION);

    MongoCollection<Document> capped = db.getCollection(CAPPED_COLLECTION);
    MongoCollection<Document> uncapped = db.getCollection(COLLECTION);
    capped.insertMany(documents);
    uncapped.insertMany(documents);

    MongoCollection<Document> uuid = db.getCollection(UUID_COLLECTION);
    uuid.insertOne(new Document("value", uuidValue));

    mongo.close();
  }

  @Test
  public void testInvalidInitialOffset() throws StageException {
    MongoSourceConfigBean configBean = new MongoSourceConfigBean();
    configBean.mongoConfig = new MongoDBConfig();
    configBean.mongoConfig.connectionString = "mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort;
    configBean.mongoConfig.database = DATABASE_NAME;
    configBean.mongoConfig.collection = CAPPED_COLLECTION;
    configBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    configBean.mongoConfig.username = null;
    configBean.mongoConfig.password = null;
    configBean.isCapped = true;
    configBean.offsetField = "_id";
    configBean.initialOffset = "0";
    configBean.batchSize = 100;
    configBean.maxBatchWaitTime = 1;
    configBean.readPreference = ReadPreferenceLabel.NEAREST;

    MongoDBSource origin = new MongoDBSource(configBean);

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());

  }

  @Test
  public void testInvalidHostname() throws StageException {
    MongoSourceConfigBean configBean = new MongoSourceConfigBean();
    configBean.mongoConfig = new MongoDBConfig();
    configBean.mongoConfig.connectionString = "mongodb://localhostsdfsd:" + mongoContainerMappedPort;
    configBean.mongoConfig.database = DATABASE_NAME;
    configBean.mongoConfig.collection = CAPPED_COLLECTION;
    configBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    configBean.mongoConfig.username = null;
    configBean.mongoConfig.password = null;
    configBean.isCapped = true;
    configBean.offsetField = "_id";
    configBean.initialOffset = "2015-06-01 00:00:00";
    configBean.batchSize = 100;
    configBean.maxBatchWaitTime = 1;
    configBean.readPreference = ReadPreferenceLabel.NEAREST;

    MongoDBSource origin = new MongoDBSource(configBean);

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());
  }

  @Test
  public void testInvalidHostPort() throws StageException {
    MongoSourceConfigBean configBean = new MongoSourceConfigBean();
    configBean.mongoConfig = new MongoDBConfig();
    configBean.mongoConfig.connectionString = "mongodb://" + mongoContainerIp;
    configBean.mongoConfig.database = DATABASE_NAME;
    configBean.mongoConfig.collection = CAPPED_COLLECTION;
    configBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    configBean.mongoConfig.username = null;
    configBean.mongoConfig.password = null;
    configBean.isCapped = true;
    configBean.offsetField = "_id";
    configBean.initialOffset = "2015-06-01 00:00:00";
    configBean.batchSize = 100;
    configBean.maxBatchWaitTime = 1;
    configBean.readPreference = ReadPreferenceLabel.NEAREST;

    MongoDBSource origin = new MongoDBSource(configBean);

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());
  }

  @Test
  public void testInvalidPort() throws StageException {
    MongoSourceConfigBean configBean = new MongoSourceConfigBean();
    configBean.mongoConfig = new MongoDBConfig();
    configBean.mongoConfig.connectionString = "mongodb://" + mongoContainerIp + ":abcd";
    configBean.mongoConfig.database = DATABASE_NAME;
    configBean.mongoConfig.collection = CAPPED_COLLECTION;
    configBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    configBean.mongoConfig.username = null;
    configBean.mongoConfig.password = null;
    configBean.isCapped = true;
    configBean.offsetField = "_id";
    configBean.initialOffset = "2015-06-01 00:00:00";
    configBean.batchSize = 100;
    configBean.maxBatchWaitTime = 1;
    configBean.readPreference = ReadPreferenceLabel.NEAREST;

    MongoDBSource origin = new MongoDBSource(configBean);

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());
  }

  @Test
  public void testReadCappedCollection() throws Exception {
    MongoSourceConfigBean configBean = new MongoSourceConfigBean();
    configBean.mongoConfig = new MongoDBConfig();
    configBean.mongoConfig.connectionString = "mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort;
    configBean.mongoConfig.database = DATABASE_NAME;
    configBean.mongoConfig.collection = CAPPED_COLLECTION;
    configBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    configBean.mongoConfig.username = null;
    configBean.mongoConfig.password = null;
    configBean.isCapped = true;
    configBean.offsetField = "_id";
    configBean.initialOffset = "2015-06-01 00:00:00";
    configBean.batchSize = 100;
    configBean.maxBatchWaitTime = 1;
    configBean.readPreference = ReadPreferenceLabel.NEAREST;

    MongoDBSource origin = new MongoDBSource(configBean);

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());

    runner.runInit();

    StageRunner.Output output = runner.runProduce(null, 2);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals(2, parsedRecords.size());
    assertEquals("document 0", parsedRecords.get(0).get("/value").getValueAsString());
    assertEquals("document 1", parsedRecords.get(1).get("/value").getValueAsString());

    for (Record r : parsedRecords) {
      LOG.info(r.toString());
    }

    String offset = output.getNewOffset();
    output = runner.runProduce(offset, 2);
    parsedRecords = output.getRecords().get("lane");
    assertEquals(2, parsedRecords.size());
    assertEquals("document 2", parsedRecords.get(0).get("/value").getValueAsString());
    assertEquals("document 3", parsedRecords.get(1).get("/value").getValueAsString());

    for (Record r : parsedRecords) {
      LOG.info(r.toString());
    }

    insertNewDocs(CAPPED_COLLECTION);

    offset = output.getNewOffset();
    output = runner.runProduce(offset, 2);
    parsedRecords = output.getRecords().get("lane");
    assertEquals(1, parsedRecords.size());
    assertEquals("document 12345", parsedRecords.get(0).get("/value").getValueAsString());
  }

  @Test
  public void testReadCollection() throws Exception {
    MongoSourceConfigBean configBean = new MongoSourceConfigBean();
    configBean.mongoConfig = new MongoDBConfig();
    configBean.mongoConfig.connectionString = "mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort;
    configBean.mongoConfig.database = DATABASE_NAME;
    configBean.mongoConfig.collection = COLLECTION;
    configBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    configBean.mongoConfig.username = null;
    configBean.mongoConfig.password = null;
    configBean.isCapped = false;
    configBean.offsetField = "_id";
    configBean.initialOffset = "2015-06-01 00:00:00";
    configBean.batchSize = 100;
    configBean.maxBatchWaitTime = 1;
    configBean.readPreference = ReadPreferenceLabel.NEAREST;

    MongoDBSource origin = new MongoDBSource(configBean);

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());

    runner.runInit();

    StageRunner.Output output = runner.runProduce(null, 2);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals(2, parsedRecords.size());
    assertEquals("document 0", parsedRecords.get(0).get("/value").getValueAsString());
    assertEquals("document 1", parsedRecords.get(1).get("/value").getValueAsString());

    for (Record r : parsedRecords) {
      LOG.info(r.toString());
    }

    String offset = output.getNewOffset();
    output = runner.runProduce(offset, 100);
    parsedRecords = output.getRecords().get("lane");
    assertEquals(2, parsedRecords.size());
    assertEquals("document 2", parsedRecords.get(0).get("/value").getValueAsString());
    assertEquals("document 3", parsedRecords.get(1).get("/value").getValueAsString());

    for (Record r : parsedRecords) {
      LOG.info(r.toString());
    }

    insertNewDocs(COLLECTION);

    offset = output.getNewOffset();
    // We have to set max batch size to N records + 1 (in this case 3) otherwise we'll
    // Need an extra produce call before a new cursor is opened.
    output = runner.runProduce(offset, 3);
    parsedRecords = output.getRecords().get("lane");
    assertEquals(1, parsedRecords.size());
    assertEquals("document 12345", parsedRecords.get(0).get("/value").getValueAsString());
  }

  @Test
  public void testReadUUIDType() throws Exception {
    MongoSourceConfigBean configBean = new MongoSourceConfigBean();
    configBean.mongoConfig = new MongoDBConfig();
    configBean.mongoConfig.connectionString = "mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort;
    configBean.mongoConfig.database = DATABASE_NAME;
    configBean.mongoConfig.collection = UUID_COLLECTION;
    configBean.mongoConfig.authenticationType = AuthenticationType.NONE;
    configBean.mongoConfig.username = null;
    configBean.mongoConfig.password = null;
    configBean.isCapped = false;
    configBean.offsetField = "_id";
    configBean.initialOffset = "2015-06-01 00:00:00";
    configBean.batchSize = 100;
    configBean.maxBatchWaitTime = 1;
    configBean.readPreference = ReadPreferenceLabel.NEAREST;

    MongoDBSource origin = new MongoDBSource(configBean);

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());

    runner.runInit();

    StageRunner.Output output = runner.runProduce(null, 1);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals(1, parsedRecords.size());
    // UUID is converted to a string.
    assertEquals(uuidValue.toString(), parsedRecords.get(0).get("/value").getValueAsString());
  }

  private void insertNewDocs(String collectionName) {
    MongoClient mongo = new MongoClient(mongoContainerIp, mongoContainerMappedPort);
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);

    MongoCollection<Document> collection = db.getCollection(collectionName);
    collection.insertOne(new Document("value", "document 12345"));

    mongo.close();
  }
}
