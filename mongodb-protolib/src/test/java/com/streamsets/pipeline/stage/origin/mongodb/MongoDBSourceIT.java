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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@Ignore
public class MongoDBSourceIT {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceIT.class);
  private static final String DATABASE_NAME = "test";
  private static final String CAPPED_COLLECTION = "capped";
  private static final String COLLECTION = "uncapped";
  private static final String UUID_COLLECTION = "uuid";
  private static final String BSON_COLLECTION = "bson";
  private static final String STRING_ID_COLLECTION = "stringId";
  private static final String CAPPED_STRING_ID_COLLECTION = "cappedStringId";
  private static final String DATE_COLLECTION = "date";
  private static final String CAPPED_DATE_COLLECTION = "cappedDate";

  private static final int TEST_COLLECTION_SIZE = 4;
  private static final int ONE_MB = 1000 * 1000;

  private static final List<Document> documents = new ArrayList<>(TEST_COLLECTION_SIZE);
  private static final UUID uuidValue = UUID.randomUUID();
  private static final int MONGO_PORT = 27017;
  private static int timestamp;

  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static final SimpleDateFormat dateFormatter = new SimpleDateFormat(TIMESTAMP_FORMAT);

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
    db.createCollection(BSON_COLLECTION);
    db.createCollection(STRING_ID_COLLECTION);
    db.createCollection(CAPPED_STRING_ID_COLLECTION, new CreateCollectionOptions().capped(true).sizeInBytes(ONE_MB));
    db.createCollection(DATE_COLLECTION);
    db.createCollection(CAPPED_DATE_COLLECTION, new CreateCollectionOptions().capped(true).sizeInBytes(ONE_MB));


    MongoCollection<Document> capped = db.getCollection(CAPPED_COLLECTION);
    MongoCollection<Document> uncapped = db.getCollection(COLLECTION);
    capped.insertMany(documents);
    uncapped.insertMany(documents);

    MongoCollection<Document> uuid = db.getCollection(UUID_COLLECTION);
    uuid.insertOne(new Document("value", uuidValue));

    MongoCollection<Document> bson = db.getCollection(BSON_COLLECTION);

    Date now = new Date();
    timestamp = (int)(now.getTime()/1000);

    bson.insertOne(new Document("value", new BsonTimestamp(timestamp, 0)));

    Map<String, Object> mapDocument = new HashMap<>();
    mapDocument.put("timestamp", new BsonTimestamp(timestamp, 1));
    bson.insertOne(new Document("valueMap", mapDocument));

    List<Object> listDocument = new ArrayList<>();
    listDocument.add(new BsonTimestamp(timestamp, 2));
    bson.insertOne(new Document("valueList", listDocument));

    mongo.close();
  }

  @Test
  public void testInvalidInitialOffset() throws StageException {
    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        .database(DATABASE_NAME)
        .collection(CAPPED_COLLECTION)
        .build();

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());

  }

  @Test
  public void testInvalidHostname() throws StageException {
    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://localhostsdfsd:" + mongoContainerMappedPort)
        .database(DATABASE_NAME)
        .collection(CAPPED_COLLECTION)
        .initialOffset("2015-06-01 00:00:00")
        .build();

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());
  }

  @Test
  public void testInvalidHostPort() throws StageException {
    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp)
        .database(DATABASE_NAME)
        .collection(CAPPED_COLLECTION)
        .initialOffset("2015-06-01 00:00:00")
        .build();

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());
  }

  @Test
  public void testInvalidPort() throws StageException {
    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":abcd")
        .database(DATABASE_NAME)
        .collection(CAPPED_COLLECTION)
        .initialOffset("2015-06-01 00:00:00")
        .build();

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    LOG.info(issues.get(0).toString());
  }

  @Test
  public void testReadCappedCollection() throws Exception {
    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        .database(DATABASE_NAME)
        .collection(CAPPED_COLLECTION)
        .initialOffset("2015-06-01 00:00:00")
        .build();

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
    assertEquals("no-more-data", runner.getEventRecords().get(0).getEventType());
  }

  @Test
  public void testReadCollection() throws Exception {
    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        .database(DATABASE_NAME)
        .collection(COLLECTION)
        .isCapped(false)
        .initialOffset("2015-06-01 00:00:00")
        .build();

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
    assertEquals("no-more-data", runner.getEventRecords().get(0).getEventType());
  }

  @Test
  public void testReadUUIDType() throws Exception {
    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        .database(DATABASE_NAME)
        .collection(UUID_COLLECTION)
        .isCapped(false)
        .initialOffset("2015-06-01 00:00:00")
        .build();

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

  @Test
  public void testReadBsonTimestampType() throws Exception {
    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        .database(DATABASE_NAME)
        .collection(BSON_COLLECTION)
        .isCapped(false)
        .initialOffset("2015-06-01 00:00:00")
        .maxBatchWaitTime(100)
        .build();

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());

    runner.runInit();

    final int maxBatchSize = 10;
    StageRunner.Output output = runner.runProduce(null, maxBatchSize);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals(3, parsedRecords.size());
    // BSON timestamp is converted into SDC map
    assertEquals(timestamp*1000L, parsedRecords.get(0).get("/value").getValueAsMap().get("timestamp").getValueAsDate().getTime());
    assertEquals(0, parsedRecords.get(0).get("/value").getValueAsMap().get("ordinal").getValueAsInteger());
  }

  @Test
  public void testReadNestedOffset() throws Exception {
    final int level = 5;
    String offsetField = "";
    for (int i = 1; i <= level; i++) {
      offsetField = offsetField + "o" + i + ".";
    }
    offsetField = offsetField + "_id";

    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        .database(DATABASE_NAME)
        .collection(COLLECTION)
        .offsetField(offsetField)
        .isCapped(false)
        .initialOffset("2015-06-01 00:00:00")
        .maxBatchWaitTime(100)
        .readPreference(ReadPreferenceLabel.NEAREST)
        .build();

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());

    runner.runInit();

    final int maxBatchSize = 10;
    StageRunner.Output output = runner.runProduce(null, maxBatchSize);
    List<Record> parsedRecords = output.getRecords().get("lane");

    assertEquals(0, parsedRecords.size());


    insertNewDocsIdInsideDoc(COLLECTION, level, TEST_COLLECTION_SIZE);

    output = runner.runProduce(null, maxBatchSize);
    parsedRecords = output.getRecords().get("lane");

    assertEquals(TEST_COLLECTION_SIZE, parsedRecords.size());
  }

  @Test
  public void testDateOffset() throws Exception {
    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":" + mongoContainerMappedPort)
        .database(DATABASE_NAME)
        .collection(DATE_COLLECTION)
        .offsetField("date")
        .isCapped(false)
        .setOffsetType(OffsetFieldType.DATE)
        .maxBatchWaitTime(100)
        .readPreference(ReadPreferenceLabel.NEAREST)
        .build();

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
            .addOutputLane("lane")
            .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());

    runner.runInit();
    insertDocsWithDateField(DATE_COLLECTION);

    final int maxBatchSize = 2;

    StageRunner.Output output = runner.runProduce(null, maxBatchSize);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals("First batch should contain 2 records",2, parsedRecords.size());

    output = runner.runProduce(output.getNewOffset(), maxBatchSize);
    parsedRecords = output.getRecords().get("lane");
    assertEquals("Second batch should contain 1 records",1, parsedRecords.size());

    output = runner.runProduce(output.getNewOffset(), maxBatchSize);
    parsedRecords = output.getRecords().get("lane");
    assertEquals("Last batch should have 0 records",0, parsedRecords.size());
  }

  @Test
  public void testDateOffsetCappedCollection() throws Exception {
    MongoDBSource origin = new MongoDBSourceBuilder()
            .connectionString("mongodb://" + mongoContainerIp + ":" + mongoContainerMappedPort)
            .database(DATABASE_NAME)
            .collection(CAPPED_DATE_COLLECTION)
            .offsetField("date")
            .isCapped(true)
            .setOffsetType(OffsetFieldType.DATE)
            .maxBatchWaitTime(100)
            .readPreference(ReadPreferenceLabel.NEAREST)
            .build();

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
            .addOutputLane("lane")
            .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());

    runner.runInit();
    insertDocsWithDateField(CAPPED_DATE_COLLECTION);

    final int maxBatchSize = 2;

    StageRunner.Output output = runner.runProduce(null, maxBatchSize);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals("First batch should contain 2 records",2, parsedRecords.size());

    output = runner.runProduce(output.getNewOffset(), maxBatchSize);
    parsedRecords = output.getRecords().get("lane");
    assertEquals("Second batch should contain 1 record",1, parsedRecords.size());

    output = runner.runProduce(output.getNewOffset(), maxBatchSize);
    parsedRecords = output.getRecords().get("lane");
    assertEquals("Last batch should have 0 records",0, parsedRecords.size());
  }

  @Test
  public void testStringOffset() throws Exception {

    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        .database(DATABASE_NAME)
        .collection(STRING_ID_COLLECTION)
        .offsetField("_id")
        .isCapped(false)
        .setOffsetType(OffsetFieldType.STRING)
        .maxBatchWaitTime(100)
        .readPreference(ReadPreferenceLabel.NEAREST)
        .build();

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());

    runner.runInit();
    insertDocsWithStringID(STRING_ID_COLLECTION);

    final int maxBatchSize = 2;
    StageRunner.Output output = runner.runProduce(null, maxBatchSize);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals("First batch should contain 2 records",2, parsedRecords.size());

    output = runner.runProduce(output.getNewOffset(), maxBatchSize);
    parsedRecords = output.getRecords().get("lane");
    assertEquals("Second batch should contain 1 records",1, parsedRecords.size());

    output = runner.runProduce(output.getNewOffset(), maxBatchSize);
    parsedRecords = output.getRecords().get("lane");
    assertEquals("Last batch should have 0 records",0, parsedRecords.size());
  }

  @Test
  public void testStringOffsetCappedCollection() throws Exception {

    MongoDBSource origin = new MongoDBSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        .database(DATABASE_NAME)
        .collection(CAPPED_STRING_ID_COLLECTION)
        .offsetField("_id")
        .isCapped(false)
        .setOffsetType(OffsetFieldType.STRING)
        .maxBatchWaitTime(100)
        .readPreference(ReadPreferenceLabel.NEAREST)
        .build();

    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());

    runner.runInit();
    insertDocsWithStringID(CAPPED_STRING_ID_COLLECTION);

    final int maxBatchSize = 2;
    StageRunner.Output output = runner.runProduce(null, maxBatchSize);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals("First batch should contain 2 records",2, parsedRecords.size());

    output = runner.runProduce(output.getNewOffset(), maxBatchSize);
    parsedRecords = output.getRecords().get("lane");
    assertEquals("Second batch should contain 2 records",1, parsedRecords.size());

    output = runner.runProduce(output.getNewOffset(), maxBatchSize);
    parsedRecords = output.getRecords().get("lane");
    assertEquals("Last batch should have 0 records",0, parsedRecords.size());
  }

  private void insertDocsWithStringID(String collectionname){
    MongoClient mongo = new MongoClient(mongoContainerIp, mongoContainerMappedPort);
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);

    MongoCollection<Document> collection = db.getCollection(collectionname);
    collection.insertOne(new Document("_id", "12345"));
    collection.insertOne(new Document("_id", "45679"));
    collection.insertOne(new Document("_id", "56789"));
    mongo.close();
  }

  private void insertNewDocs(String collectionName) {
    MongoClient mongo = new MongoClient(mongoContainerIp, mongoContainerMappedPort);
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);

    MongoCollection<Document> collection = db.getCollection(collectionName);
    collection.insertOne(new Document("value", "document 12345"));

    mongo.close();
  }

  private void insertDocsWithDateField(String collectionName) throws Exception
  {
    MongoClient mongo = new MongoClient(mongoContainerIp, mongoContainerMappedPort);
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);

    MongoCollection<Document> collection = db.getCollection(collectionName);
    collection.insertOne(new Document("date", dateFormatter.parse("2015-06-01 00:00:00")));
    collection.insertOne(new Document("date", dateFormatter.parse("2015-06-02 00:00:00")));
    collection.insertOne(new Document("date", dateFormatter.parse("2015-06-03 00:00:00")));

    mongo.close();
  }

  private void insertNewDocsIdInsideDoc(String collectionName, int level, int size) {
    MongoClient mongo = new MongoClient(mongoContainerIp, mongoContainerMappedPort);
    MongoDatabase db = mongo.getDatabase(DATABASE_NAME);

    MongoCollection<Document> collection = db.getCollection(collectionName);

    List<Document> docs = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      Document document = new Document("o" + level, new Document("_id", new ObjectId()));
      for (int j = level-1; j > 0; j--) {
        document = new Document("o" + j, document);
      }

      docs.add(document);
    }

    collection.insertMany(docs);

    mongo.close();
  }
}
