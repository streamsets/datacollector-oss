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

import com.google.common.collect.Sets;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;
import com.streamsets.pipeline.stage.origin.mongodb.MongoDBSource;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

@Ignore
public class MongoDBOplogSourceIT {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDBOplogSourceIT.class);
  private static final String OPLOG_COLLECTION = "oplog.rs";
  private static final String LANE = "lane";

  private static final String DATABASE = "TEST_DB";

  @ClassRule
  public static GenericContainer mongoContainer = new GenericContainer("mongo:2.6")
      .withExposedPorts(MongoDBConfig.MONGO_DEFAULT_PORT);

  @Rule public TestName name = new TestName();

  private static int mongoContainerMappedPort = 0;
  private static String mongoContainerIp = null;
  private static MongoClient mongoClient = null;

  private int initialTs;
  private String testCollectionName;
  private MongoCollection<Document> testDocuments;
  private FindIterable<Document> mongoCursorFindIterable;

  @BeforeClass
  public static void setUpClass() throws Exception {
    LOG.info("Initializing");
    mongoContainerMappedPort = mongoContainer.getMappedPort(MongoDBConfig.MONGO_DEFAULT_PORT);
    mongoContainerIp = mongoContainer.getContainerIpAddress();//192.168.99.101
    mongoClient = new MongoClient(mongoContainerIp, mongoContainerMappedPort);
  }

  @Before
  public void createCollection() throws Exception {
    MongoDatabase db = mongoClient.getDatabase(DATABASE);
    testCollectionName = name.getMethodName();
    db.createCollection(testCollectionName);
    final long currentTime = System.currentTimeMillis();
    //To make sure that oplog is read on each method after we created the above collection.
    //We let this current second pass, before we get the initial timestamp seconds.
    Awaitility.await().untilTrue(new AtomicBoolean((System.currentTimeMillis() - currentTime) > 1000));
    //So we can skip old oplogs and just start with whatever this test is producing
    initialTs = getInitialTsFromCurrentTime();
    testDocuments = mongoClient.getDatabase(DATABASE).getCollection(testCollectionName);
    mongoCursorFindIterable = mongoClient.getDatabase("local").getCollection(OPLOG_COLLECTION)
        .find()
        //As the collection is a capped collection we use Tailable cursor which will return results in natural order in this case
        //based on ts timestamp field.
        //Tailable Await does not return and blocks, so we are using tailable.
        .cursorType(CursorType.Tailable);
  }

  private void checkRecordForFields(Record record, String opType, String ns) {
    for (String mandatoryField : MongoDBOplogSource.MANDATORY_FIELDS_IN_RECORD) {
      Assert.assertTrue(record.has("/" + mandatoryField));
    }

    Assert.assertNotNull(record.getHeader().getAttribute(MongoDBOplogSource.NS_FIELD));
    Assert.assertNotNull(record.getHeader().getAttribute(MongoDBOplogSource.OP_TYPE_FIELD));

    Assert.assertEquals(opType, record.getHeader().getAttribute(MongoDBOplogSource.OP_TYPE_FIELD));
    Assert.assertEquals(ns, record.getHeader().getAttribute(MongoDBOplogSource.NS_FIELD));

    Assert.assertEquals(Field.Type.MAP, record.get("/" + MongoDBOplogSource.TIMESTAMP_FIELD).getType());
    Assert.assertEquals(Field.Type.LONG, record.get("/" + MongoDBOplogSource.OP_LONG_HASH_FIELD).getType());
    Assert.assertEquals(Field.Type.STRING, record.get("/" + MongoDBOplogSource.OP_TYPE_FIELD).getType());
    Assert.assertEquals(Field.Type.STRING, record.get("/" + MongoDBOplogSource.NS_FIELD).getType());
    Assert.assertEquals(Field.Type.MAP, record.get("/" + MongoDBOplogSource.OP_FIELD).getType());
    Assert.assertEquals(Field.Type.INTEGER, record.get("/" + MongoDBOplogSource.VERSION_FIELD).getType());

    Map<String, Field> timeStampField = record.get("/" + MongoDBOplogSource.TIMESTAMP_FIELD).getValueAsMap();
    Assert.assertTrue(timeStampField.keySet().containsAll(Arrays.asList(MongoDBUtil.BSON_TS_TIME_T_FIELD, MongoDBUtil.BSON_TS_ORDINAL_FIELD)));

    Assert.assertEquals(2,record.get("/" + MongoDBOplogSource.VERSION_FIELD).getValueAsInteger());

    Assert.assertEquals(opType, record.get("/" + MongoDBOplogSource.OP_TYPE_FIELD).getValueAsString());
    Assert.assertEquals(ns, record.get("/" + MongoDBOplogSource.NS_FIELD).getValueAsString());
  }

  private String getMismatchMessage(String fieldName, Field.Type type, String anyReason) {
    return Utils.format("Mismatch in '{}' of type '{}' {}", fieldName, type, anyReason);
  }

  private void walkAndCheckField(String fieldName, Field expected, Field actual) {
    Assert.assertEquals(expected.getType(), actual.getType());
    Field.Type type = expected.getType();
    switch (type) {
      case MAP:
      case LIST_MAP:
        Map<String, Field> expectedFieldMap = expected.getValueAsMap();
        Map<String, Field> actualFieldMap = actual.getValueAsMap();
        Assert.assertTrue(getMismatchMessage(fieldName, type, "Expected Field has different keys from Actual Field"), Sets.difference(expectedFieldMap.keySet(), actualFieldMap.keySet()).isEmpty());
        Assert.assertTrue(getMismatchMessage(fieldName, type, "Actual Field has different keys from Expected Field"), Sets.difference(actualFieldMap.keySet(), expectedFieldMap.keySet()).isEmpty());
        for (String key : expectedFieldMap.keySet()) {
          walkAndCheckField(fieldName + "/" + key, expectedFieldMap.get(key), actualFieldMap.get(key));
        }
        break;
      case LIST:
        List<Field> expectedFieldList = expected.getValueAsList();
        List<Field> actualFieldList = actual.getValueAsList();
        Assert.assertEquals(getMismatchMessage(fieldName, type, "Size mistmatch"), expectedFieldList.size(), actualFieldList.size());
        for (int i=0; i < expectedFieldList.size(); i++) {
          walkAndCheckField(fieldName + "[" + i + "]", expectedFieldList.get(i), actualFieldList.get(i));
        }
        break;
      case BYTE_ARRAY:
        Assert.assertArrayEquals(getMismatchMessage(fieldName, type, "Byte Array value mismatch"), expected.getValueAsByteArray(), actual.getValueAsByteArray());
        break;
      default:
        Assert.assertEquals(getMismatchMessage(fieldName, type, "Field Value mismatch"),expected.getValue(), actual.getValue());
    }
  }

  private void checkRecord(Record record, Document document) throws Exception {
    Field expectedField = Field.create(MongoDBUtil.createFieldFromDocument(document));
    walkAndCheckField("", expectedField, record.get());
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("mongo", false, OnRecordError.DISCARD, Collections.emptyList());
  }

  private Pair<String, List<Record>> runSourceAndGetOp(SourceRunner runner, String offset) throws Exception {
    StageRunner.Output op = runner.runProduce(offset, 1000);
    offset = op.getNewOffset();
    List<Record> records = op.getRecords().get(LANE);
    return Pair.of(offset, records);
  }

  private int getInitialTsFromCurrentTime() {
    return (int) (Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() / 1000);
  }

  @Test
  public void testMultipleOps() throws Exception {
    MongoDBOplogSource source = new MongoDBOplogSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        //Skip old oplogs and just start with whatever this test is producing
        .initialTs(initialTs)
        .initialOrdinal(0)
        .collection(OPLOG_COLLECTION)
        .build();
    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, source)
        .addOutputLane(LANE)
        .build();
    MongoCursor<Document> cursor =
        mongoCursorFindIterable
            .filter(
                Filters.and(
                    Filters.gt(MongoDBOplogSource.TIMESTAMP_FIELD, new BsonTimestamp(initialTs, 0)),
                    Filters.or(
                        Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.INSERT.getOp()),
                        Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.UPDATE.getOp()),
                        Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.DELETE.getOp())
                    )
                )
            )
            .iterator();
    runner.runInit();
    String offset = "";
    List<Record> records;
    try {
      //insert some testDocuments in collection1
      Document document1 = new Document("a", 1);
      Document document2 = new Document("a", 2);
      Document document3 = new Document("a", 3);

      testDocuments.insertMany(Arrays.asList(document1, document2, document3));

      Pair<String, List<Record>> runOp = runSourceAndGetOp(runner, offset);
      offset = runOp.getLeft();
      records = runOp.getRight();
      Assert.assertEquals(3, records.size());
      for (Record record : records) {
        checkRecord(record, cursor.tryNext());
        checkRecordForFields(record, OplogOpType.INSERT.getOp(), DATABASE + "." + testCollectionName);
      }

      //update a document
      Bson filter = Filters.eq("a", 1);
      Bson newValue = new Document("a", 5.0);
      Bson updateOperationDocument = new Document("$set", newValue);
      UpdateResult updateResult = testDocuments.updateOne(filter, updateOperationDocument);
      Assert.assertEquals(1, updateResult.getModifiedCount());

      runOp = runSourceAndGetOp(runner, offset);
      offset = runOp.getLeft();
      records = runOp.getRight();
      Assert.assertEquals(1, records.size());
      for (Record record : records) {
        checkRecord(record, cursor.tryNext());
      }

      //Delete a document
      DeleteResult deleteResult = testDocuments.deleteOne(document2);
      Assert.assertEquals(1, deleteResult.getDeletedCount());

      runOp = runSourceAndGetOp(runner, offset);
      records = runOp.getRight();
      Assert.assertEquals(1, records.size());
      for (Record record : records) {
        checkRecord(record, cursor.tryNext());
      }
    } finally {
      runner.runDestroy();
      cursor.close();
    }
  }

  @Test
  public void testWithOnlyUpdateFilter() throws Exception {
    MongoDBOplogSource source = new MongoDBOplogSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        .collection(OPLOG_COLLECTION)
        //Skip old oplogs and just start with whatever this test is producing
        .initialTs(initialTs)
        .initialOrdinal(0)
        .filterOlogOpTypeFilter(Collections.singletonList(OplogOpType.UPDATE))
        .build();
    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, source)
        .addOutputLane(LANE)
        .build();
    MongoCursor<Document> cursor =
        mongoCursorFindIterable
            .filter(
                Filters.and(
                    Filters.gt(MongoDBOplogSource.TIMESTAMP_FIELD, new BsonTimestamp(initialTs, 0)),
                    Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.UPDATE.getOp())
                )
            )
            .iterator();
    runner.runInit();
    String offset = "";
    List<Record> records;
    try {
      //insert some testDocuments in collection1
      Document document1 = new Document("b", 1);
      Document document2 = new Document("b", 2);
      Document document3 = new Document("b", 3);

      //Insert should not yield any recods.
      testDocuments.insertMany(Arrays.asList(document1, document2, document3));

      //delete should not yield any record, deleted document 1 - we have document 2 and 3
      DeleteResult deleteResult = testDocuments.deleteOne(new Document("b", 1));
      Assert.assertEquals(1, deleteResult.getDeletedCount());

      //Insert and delete will not yield any records
      Pair<String, List<Record>> runOp = runSourceAndGetOp(runner, offset);
      offset = runOp.getLeft();
      records = runOp.getRight();
      Assert.assertEquals(0, records.size());

      //update testDocuments with key b greater than 1, will match 2 testDocuments
      Bson filter = Filters.gt("b", 1);
      Bson updateOperationDocument = new Document("$set", new Document("b", 7));
      UpdateResult updateResult = testDocuments.updateMany(filter, updateOperationDocument);
      Assert.assertEquals(2, updateResult.getModifiedCount());

      runOp = runSourceAndGetOp(runner, offset);
      records = runOp.getRight();
      Assert.assertEquals(2, records.size());
      for (Record record : records) {
        checkRecord(record, cursor.tryNext());
        checkRecordForFields(record, OplogOpType.UPDATE.getOp(), DATABASE + "." + testCollectionName);
      }
    } finally {
      runner.runDestroy();
      cursor.close();
    }
  }

  @Test
  public void testWithOnlyCmdFilter() throws Exception {
    MongoDBOplogSource source = new MongoDBOplogSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        //Skip old oplogs and just start with whatever this test is producing
        .initialTs(getInitialTsFromCurrentTime())
        .initialOrdinal(0)
        .collection(OPLOG_COLLECTION)
        //Just filter update oplogs
        .filterOlogOpTypeFilter(Collections.singletonList(OplogOpType.CMD))
        .initialTs(initialTs)
        .initialOrdinal(0)
        .build();
    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, source)
        .addOutputLane(LANE)
        .build();
    MongoCursor<Document> cursor =
        mongoCursorFindIterable
            .filter(
                Filters.and(
                    Filters.gt(MongoDBOplogSource.TIMESTAMP_FIELD, new BsonTimestamp(initialTs, 0)),
                    Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.CMD.getOp())
                )
            )
            .iterator();
    runner.runInit();
    String offset = "";
    List<Record> records;
    try {
      //insert some testDocuments in collection1
      Document document1 = new Document("c", 1);
      Document document2 = new Document("c", 2);
      Document document3 = new Document("c", 3);
      Document document4 = new Document("c", 4);
      Document document5 = new Document("c", 5);
      Document document6 = new Document("c", 6);
      Document document7 = new Document("c", 7);

      testDocuments.insertMany(Arrays.asList(document1, document2, document3, document4, document5, document6, document7));

      //Delete two records
      DeleteResult deleteResult = testDocuments.deleteMany(Filters.gt("c", 5));
      Assert.assertEquals(2, deleteResult.getDeletedCount());

      //Update by Incrementing the field "c" by 1 for testDocuments 1, 2 and 3
      UpdateResult updateResult = testDocuments.updateMany(Filters.lt("c", 4), new Document("$inc", new Document("c", 1)));
      Assert.assertEquals(3, updateResult.getModifiedCount());

      //Now create bunch of collections, these are the only records we should see.
      int numberOfCollectionsToCreate = 5;
      for (int i = 0; i < numberOfCollectionsToCreate; i++) {
        mongoClient.getDatabase(DATABASE).createCollection(testCollectionName + "_" + i);
      }

      Pair<String, List<Record>> runOp = runSourceAndGetOp(runner, offset);
      records = runOp.getRight();
      //Only testDocuments with "CMD" op should be selected
      Assert.assertEquals(5, records.size());
      for (Record record : records) {
        checkRecord(record, cursor.tryNext());
        checkRecordForFields(record, OplogOpType.CMD.getOp(), DATABASE + ".$cmd");
      }
    } finally {
      runner.runDestroy();
      cursor.close();
    }
  }

  @Test
  public void testReadFromOffset() throws Exception {
    MongoDBOplogSource source = new MongoDBOplogSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        .collection(OPLOG_COLLECTION)
        .initialTs(initialTs)
        .initialOrdinal(0)
        .build();
    SourceRunner runner = new SourceRunner.Builder(MongoDBSource.class, source)
        .addOutputLane(LANE)
        .build();
    MongoCursor<Document> cursor =
        mongoCursorFindIterable
            .filter(
                Filters.and(
                    Filters.gt(MongoDBOplogSource.TIMESTAMP_FIELD, new BsonTimestamp(initialTs, 0)),
                    Filters.or(
                        Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.INSERT.getOp()),
                        Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.UPDATE.getOp()),
                        Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.DELETE.getOp()),
                        Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.CMD.getOp()),
                        Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.NOOP.getOp()),
                        Filters.eq(MongoDBOplogSource.OP_TYPE_FIELD, OplogOpType.DB.getOp())
                    )
                )
            )
            .iterator();
    //Insert 3 testDocuments
    Document document1 = new Document("d", 1);
    Document document2 = new Document("d", 2);
    Document document3 = new Document("d", 3);
    testDocuments.insertMany(Arrays.asList(document1, document2, document3));

    runner.runInit();
    String offset = "";
    List<Record> records;
    try {
      Pair<String, List<Record>> runOp = runSourceAndGetOp(runner, offset);
      records = runOp.getRight();
      //Persist offset from this run and use it.
      offset = runOp.getLeft();
      Assert.assertEquals(3, records.size());
      for (Record record : records) {
        checkRecord(record, cursor.tryNext());
        checkRecordForFields(record, OplogOpType.INSERT.getOp(), DATABASE + "." + testCollectionName);
      }
    } finally {
      runner.runDestroy();
    }

    //Now update the testDocuments by incrementing the field value of d by 1 for first 2 testDocuments
    UpdateResult updateResult = testDocuments.updateMany(Filters.lt("d", 3), new Document("$inc", new Document("c", 1)));
    Assert.assertEquals(2, updateResult.getModifiedCount());

    //Recreate runner and source but use the previous offset
    source = new MongoDBOplogSourceBuilder()
        .connectionString("mongodb://" + mongoContainerIp + ":"  + mongoContainerMappedPort)
        //No initial offset (ts/ordinal)
        .collection(OPLOG_COLLECTION)
        .filterOlogOpTypeFilter(Collections.singletonList(OplogOpType.UPDATE))
        .build();
    runner = new SourceRunner.Builder(MongoDBSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();
    try {
      Pair<String, List<Record>> runOp = runSourceAndGetOp(runner, offset);
      records = runOp.getRight();
      Assert.assertEquals(2, records.size());
      for (Record record : records) {
        checkRecord(record, cursor.tryNext());
        checkRecordForFields(record, OplogOpType.UPDATE.getOp(), DATABASE + "." + testCollectionName);
      }
    } finally {
      runner.runDestroy();
      cursor.close();
    }
  }

  @AfterClass
  public static void destroy() throws Exception {
    mongoClient.dropDatabase(DATABASE);
    mongoClient.close();
  }
}
