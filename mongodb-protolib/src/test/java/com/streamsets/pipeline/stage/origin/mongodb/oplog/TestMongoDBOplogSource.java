/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.mongodb.oplog;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestMongoDBOplogSource {

  private void testInvalidConfiguration(int initialTs, int initialOrdinal, String collection) throws Exception {
    MongoDBOplogSource source = new MongoDBOplogSourceBuilder()
        .connectionString("mongodb://localhost:27017")
        .initialTs(initialTs)
        .initialOrdinal(initialOrdinal)
        .collection("oplog.rs")
        .initialTs(-1)
        .initialOrdinal(0)
        .build();
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    source.extraInit(ContextInfoCreator.createSourceContext("a", false, OnRecordError.TO_ERROR, Collections.<String>emptyList()), issues);
    Assert.assertFalse(issues.isEmpty());
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testInvalidInitialTs() throws Exception {
    testInvalidConfiguration( -1, 1, "oplog.rs");
  }

  @Test
  public void testInvalidInitialOrdinal() throws Exception {
    testInvalidConfiguration( (int) (System.currentTimeMillis()/1000), -1, "oplog.rs");
  }

  @Test
  public void testInvalidOplogCollection() throws Exception {
    testInvalidConfiguration( -1, -1, "random_collection");
  }

  public void testGenericOpTypesWithPopulateGenericOpTypeInHeader(String op, String expectedOperationTypeLabel) throws Exception {
    Record record = RecordCreator.create();
    MongoDBOplogSource.populateGenericOperationTypeInHeader(record, op);
    Assert.assertNotNull(record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE));
    Assert.assertEquals(expectedOperationTypeLabel, record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE));
  }

  public void testNonGenericOpTypesWithPopulateGenericOpTypeInHeader(String op) throws Exception {
    Record record = RecordCreator.create();
    MongoDBOplogSource.populateGenericOperationTypeInHeader(record, op);
    Assert.assertNull(record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE));
  }

  @Test
  public void testGenericOpLogRecordFromTheDocumentInsert() throws Exception {
    testGenericOpTypesWithPopulateGenericOpTypeInHeader(OplogOpType.INSERT.getOp(), OperationType.getLabelFromIntCode(OperationType.INSERT_CODE));
  }

  @Test
  public void testGenericOpLogRecordFromTheDocumentUpdate() throws Exception {
    testGenericOpTypesWithPopulateGenericOpTypeInHeader(OplogOpType.UPDATE.getOp(), OperationType.getLabelFromIntCode(OperationType.UPDATE_CODE));
  }

  @Test
  public void testGenericOpLogRecordFromTheDocumentDelete() throws Exception {
    testGenericOpTypesWithPopulateGenericOpTypeInHeader(OplogOpType.DELETE.getOp(), OperationType.getLabelFromIntCode(OperationType.DELETE_CODE));
  }

  @Test
  public void testGenericOpLogRecordFromTheDocumentCMD() throws Exception {
    testNonGenericOpTypesWithPopulateGenericOpTypeInHeader(OplogOpType.CMD.getOp());
  }

  @Test
  public void testGenericOpLogRecordFromTheDocumentNOOP() throws Exception {
    testNonGenericOpTypesWithPopulateGenericOpTypeInHeader(OplogOpType.NOOP.getOp());
  }

  @Test
  public void testGenericOpLogRecordFromTheDocumentDB() throws Exception {
    testNonGenericOpTypesWithPopulateGenericOpTypeInHeader(OplogOpType.DB.getOp());
  }

  @Test
  public void testInvalidOpTypeWithPopulateGenericOperationTypeInHeader() throws Exception {
    Record record = RecordCreator.create();
    String op = "unknown";
    try {
      MongoDBOplogSource.populateGenericOperationTypeInHeader(record, op);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(op));
    }
  }


}
