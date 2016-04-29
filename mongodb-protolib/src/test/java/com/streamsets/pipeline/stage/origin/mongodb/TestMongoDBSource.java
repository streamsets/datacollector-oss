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
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestMongoDBSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestMongoDBSource.class);
  private static final String DATABASE_NAME = "test";
  private static final String CAPPED_COLLECTION = "capped";
  private static final String COLLECTION = "uncapped";
  private static final String UUID_COLLECTION = "uuid";
  private static final int TEST_COLLECTION_SIZE = 4;
  private static final int ONE_MB = 1000 * 1000;

  private static final List<Document> documents = new ArrayList<>(TEST_COLLECTION_SIZE);
  private static final UUID uuidValue = UUID.randomUUID();
  private static MongodExecutable mongodExecutable = null;
  private static int port = 0;

  @BeforeClass
  public static void setUpClass() throws Exception {
    for (int i = 0; i < TEST_COLLECTION_SIZE; i++) {
      documents.add(new Document("value", "document " + i));
    }

    MongodStarter starter = MongodStarter.getDefaultInstance();

    ServerSocket s = new ServerSocket(0);
    port = s.getLocalPort();
    s.close();

    IMongodConfig mongodConfig = new MongodConfigBuilder()
        .version(Version.Main.PRODUCTION)
        .net(new Net(port, Network.localhostIsIPv6()))
        .build();


    mongodExecutable = starter.prepare(mongodConfig);
    mongodExecutable.start();

    MongoClient mongo = new MongoClient("localhost", port);
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

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (mongodExecutable != null)
      mongodExecutable.stop();
  }

  @Test
  public void testInvalidInitialOffset() throws StageException {
    MongoDBSource origin = new MongoDBSource(
        "mongodb://localhost:" + port,
        DATABASE_NAME,
        CAPPED_COLLECTION,
        true,
        "_id",
        "0",
        100,
        1,
        AuthenticationType.NONE,
        null,
        null,
        ReadPreference.nearest()
    );

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());

  }

  @Test
  public void testInvalidHostname() throws StageException {
    MongoDBSource origin = new MongoDBSource(
        "mongodb://localhostsdfsd:" + port,
        DATABASE_NAME,
        CAPPED_COLLECTION,
        true,
        "_id",
        "2015-06-01 00:00:00",
        100,
        1,
        AuthenticationType.NONE,
        null,
        null,
        ReadPreference.nearest()
    );

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());
  }

  @Test
  public void testInvalidHostPort() throws StageException {
    MongoDBSource origin = new MongoDBSource(
        "mongodb://localhost",
        DATABASE_NAME,
        CAPPED_COLLECTION,
        true,
        "_id",
        "2015-06-01 00:00:00",
        100,
        1,
        AuthenticationType.NONE,
        null,
        null,
        ReadPreference.nearest()
    );

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());
  }

  @Test
  public void testInvalidPort() throws StageException {
    MongoDBSource origin = new MongoDBSource(
        "mongodb://localhost:abcd",
        DATABASE_NAME,
        CAPPED_COLLECTION,
        true,
        "_id",
        "2015-06-01 00:00:00",
        100,
        1,
        AuthenticationType.NONE,
        null,
        null,
        ReadPreference.nearest()
    );

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());
  }

  @Test
  public void testReadCappedCollection() throws Exception {
    MongoDBSource origin = new MongoDBSource(
        "mongodb://localhost:" + port,
        DATABASE_NAME,
        CAPPED_COLLECTION,
        true,
        "_id",
        "2015-06-01 00:00:00",
        100,
        1,
        AuthenticationType.NONE,
        null,
        null,
        ReadPreference.nearest()
    );

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

  @Ignore
  @Test
  public void testReadCollection() throws Exception {
    MongoDBSource origin = new MongoDBSource(
        "mongodb://localhost:" + port,
        DATABASE_NAME,
        COLLECTION,
        false,
        "_id",
        "2015-06-01 00:00:00",
        100,
        1,
        AuthenticationType.NONE,
        null,
        null,
        ReadPreference.nearest()
    );

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
    MongoDBSource origin = new MongoDBSource(
        "mongodb://localhost:" + port,
        DATABASE_NAME,
        UUID_COLLECTION,
        false,
        "_id",
        "2015-06-01 00:00:00",
        100,
        1,
        AuthenticationType.NONE,
        null,
        null,
        ReadPreference.nearest()
    );

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
    MongoClient mongo = new MongoClient("localhost", port);
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);

    MongoCollection<Document> collection = db.getCollection(collectionName);
    collection.insertOne(new Document("value", "document 12345"));

    mongo.close();
  }
}
