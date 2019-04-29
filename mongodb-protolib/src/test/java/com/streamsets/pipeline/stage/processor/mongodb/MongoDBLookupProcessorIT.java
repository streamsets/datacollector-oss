/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.junit.Assert;

public class MongoDBLookupProcessorIT {

  private static MongoClient mongo = null;

  private static final String DATABASE_NAME = "testDatabase1";
  private static final String TEST_COLLECTION = "testCollection";
  private static MongoCollection<Document> testCollection = null;
  private MongoDBProcessorBuilder builder;

  @ClassRule
  public static GenericContainer mongoContainer = new GenericContainer("mongo:3.0").withExposedPorts(MongoDBConfig.MONGO_DEFAULT_PORT);

  @BeforeClass
  public static void setUpClass() throws Exception {
    mongo = new MongoClient(mongoContainer.getContainerIpAddress(), mongoContainer.getMappedPort(MongoDBConfig.MONGO_DEFAULT_PORT));
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    mongo.close();
  }
  @Before
  public void setUp() throws Exception {
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);
    db.createCollection(TEST_COLLECTION);
    testCollection = db.getCollection(TEST_COLLECTION);
    testCollection.insertOne(
        new Document()
            .append("id", 1)
            .append("name", "StreamSets")
            .append("location", "Fan Francisco")
    );
    testCollection.insertOne(
        new Document()
            .append("id", 2)
            .append("name", "MongoDB")
            .append("location", "Palo Alto")
    );
    builder = new MongoDBProcessorBuilder();
    builder.connectionString("mongodb://" + mongoContainer.getContainerIpAddress() + ":" + mongoContainer.getMappedPort(MongoDBConfig.MONGO_DEFAULT_PORT));
    builder.database(DATABASE_NAME);
    builder.collection(TEST_COLLECTION);
  }

  @After
  public void tearDown() throws Exception {
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);
    db.getCollection(TEST_COLLECTION).drop();
  }

  @Test
  public void testConfig() {
    // Config issue for filter (SDC Field to Document field) not provided
    MongoDBProcessor processor = builder.resultField("/lookupResult")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(MongoDBDProcessor.class, processor)
        .addOutputLane("mongo").build();
    try {
      runner.runInit();
      Assert.fail("Should throw StageException");
    } catch (StageException e) {
      // pass
    }
  }

  @Test
  public void testSimpleLookup() throws StageException {
    MongoDBProcessor processor = builder.resultField("/lookupResult")
        .addFieldMapping(new MongoDBFieldColumnMapping("id", "/id"))
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(MongoDBDProcessor.class, processor)
        .addOutputLane("mongo").build();
    runner.runInit();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("id", Field.create(1));
    map.put("name", Field.create("StreamSets"));
    record.set(Field.create(map));
    List<Record> output = runner.runProcess(ImmutableList.of(record)).getRecords().get("mongo");

    Record result = output.get(0);
    Assert.assertEquals("StreamSets", result.get("/lookupResult/name").getValueAsString());
  }
}
