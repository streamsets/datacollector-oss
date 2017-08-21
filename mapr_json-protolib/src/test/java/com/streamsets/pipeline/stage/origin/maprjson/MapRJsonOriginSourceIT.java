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
package com.streamsets.pipeline.stage.origin.maprjson;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.exceptions.DBException;
import com.mapr.org.apache.hadoop.hbase.util.Bytes;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.ojai.Document;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

@Category(SingleForkNoReuseTest.class)
@Ignore
public class MapRJsonOriginSourceIT {

  private static final String SOME_JSON = "{ \"aa\": \"hello\", \"bb\": \"world\", \"num\": 12345 }";
  private static final String TABLE_NAME = "mytable";

  private void basicConfiguration(MapRJsonOriginConfigBean conf) {

    conf.tableName = TABLE_NAME;
    conf.startValue = "";

  }


@After
  public void deleteTable() {
    try {
      MapRDB.deleteTable(TABLE_NAME);
    } catch (DBException ex) {
      throw ex;
    }
  }

  @Test
  public void testTableName() {

    MapRJsonOriginConfigBean conf = new MapRJsonOriginConfigBean();
    basicConfiguration(conf);

    MapRJsonOriginSource mapr = new MapRJsonOriginSource(conf);
    assertEquals(TABLE_NAME, conf.tableName);

  }

  @Test
  public void testTableDoesNotExist() {

    MapRJsonOriginConfigBean conf = new MapRJsonOriginConfigBean();
    basicConfiguration(conf);

    MapRJsonOriginSource mapr = new MapRJsonOriginSource(conf);

    SourceRunner runner = new SourceRunner.Builder(MapRJsonOriginSource.class, mapr)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.DISCARD)
        .build();

    try {
      List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
      assertEquals(1, issues.size());
      assertTrue(issues.get(0).toString().contains("MAPR_JSON_ORIGIN_06"));
    } catch(Exception e) {
      e.printStackTrace();
    }

  }

  @Test(timeout = 10000)
  public void testConnect() throws Exception {

    MapRJsonOriginConfigBean conf = new MapRJsonOriginConfigBean();
    basicConfiguration(conf);

    MapRJsonOriginSource source = new MapRJsonOriginSource(conf);

    SourceRunner runner = new SourceRunner.Builder(MapRJsonOriginDSource.class,
        source
    ).setOnRecordError(OnRecordError.DISCARD).addOutputLane("lane").build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("MAPR_JSON_ORIGIN_06"));

  }

  @Test(timeout = 10000)
  public void testReadADocument() throws Exception {

    insertRows(SOME_JSON, 1, false);

    MapRJsonOriginConfigBean conf = new MapRJsonOriginConfigBean();
    basicConfiguration(conf);

    MapRJsonOriginSource source = new MapRJsonOriginSource(conf);

    SourceRunner runner = new SourceRunner.Builder(MapRJsonOriginDSource.class,
        source
    ).setOnRecordError(OnRecordError.DISCARD).addOutputLane("lane").build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    runner.runInit();

    final int maxBatchSize = 100;
    StageRunner.Output output = runner.runProduce(null, maxBatchSize);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals("Batch should contain 1 record", 1, parsedRecords.size());
    Record rec = parsedRecords.get(0);

    assertEquals(rec.get("/_id").getValueAsString(), "0");
    assertEquals(rec.get("/aa").getValueAsString(), "hello");
    assertEquals(rec.get("/bb").getValueAsString(), "world");
    assertEquals(rec.get("/num").getValueAsLong(), 12345);

  }

  @Test(timeout = 10000)
  public void testReadADocumentBinaryKey() throws Exception {

    insertRows(SOME_JSON, 1, true);

    MapRJsonOriginConfigBean conf = new MapRJsonOriginConfigBean();
    basicConfiguration(conf);

    MapRJsonOriginSource source = new MapRJsonOriginSource(conf);

    SourceRunner runner = new SourceRunner.Builder(MapRJsonOriginDSource.class,
        source
    ).setOnRecordError(OnRecordError.DISCARD).addOutputLane("lane").build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    runner.runInit();

    final int maxBatchSize = 100;
    StageRunner.Output output = runner.runProduce(null, maxBatchSize);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals("Batch should contain 1 record", 1, parsedRecords.size());
    Record rec = parsedRecords.get(0);

    assertEquals(rec.get("/_id").getValueAsString(), "0");
    assertEquals(rec.get("/aa").getValueAsString(), "hello");
    assertEquals(rec.get("/bb").getValueAsString(), "world");
    assertEquals(rec.get("/num").getValueAsLong(), 12345);

  }

  @Test(timeout = 10000)
  public void testReadingMultipleBatches() throws Exception {
    final int numDocs = 100;

    insertRows(SOME_JSON, numDocs, false);

    MapRJsonOriginConfigBean conf = new MapRJsonOriginConfigBean();
    basicConfiguration(conf);

    MapRJsonOriginSource source = new MapRJsonOriginSource(conf);

    SourceRunner runner = new SourceRunner.Builder(MapRJsonOriginDSource.class,
        source
    ).setOnRecordError(OnRecordError.DISCARD).addOutputLane("lane").build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    runner.runInit();

    // keys will be in alpha order.
    String[] keys = new String[numDocs];
    for (int i = 0; i < numDocs; i++) {
      keys[i] = "" + i;
    }
    Arrays.sort(keys);

    final int maxBatchSize = 10;
    for (int i = 0; i < numDocs / maxBatchSize; i++) {
      StageRunner.Output output = runner.runProduce(null, maxBatchSize);
      List<Record> parsedRecords = output.getRecords().get("lane");
      assertEquals("Batch should contain 10 records", 10, parsedRecords.size());

      for (int j = 0; j < maxBatchSize; j++) {
        Record rec = parsedRecords.get(j);
        assertEquals(rec.get("/_id").getValueAsString(), keys[i * maxBatchSize + j]);
        assertEquals(rec.get("/aa").getValueAsString(), "hello");
        assertEquals(rec.get("/bb").getValueAsString(), "world");
        assertEquals(rec.get("/num").getValueAsLong(), 12345);
      }
    }

  }

  @Test(timeout = 10000)
  public void testReadingSpecificDocument() throws Exception {
    final int NUMDOCS = 100;
    final int STARTPOINT = 50;

    insertRows(SOME_JSON, NUMDOCS, false);

    MapRJsonOriginConfigBean conf = new MapRJsonOriginConfigBean();
    basicConfiguration(conf);
    conf.startValue = "50";

    MapRJsonOriginSource source = new MapRJsonOriginSource(conf);

    SourceRunner runner = new SourceRunner.Builder(MapRJsonOriginDSource.class,
        source
    ).setOnRecordError(OnRecordError.DISCARD).addOutputLane("lane").build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    runner.runInit();

    // keys need to be in alpha order.
    String[] keys = new String[NUMDOCS - STARTPOINT];
    for (int i = 0; i < NUMDOCS - STARTPOINT; i++) {
      keys[i] = "" + (i + STARTPOINT);
    }
    Arrays.sort(keys);

    final int maxBatchSize = 10;
    StageRunner.Output output = runner.runProduce(null, maxBatchSize);
    List<Record> parsedRecords = output.getRecords().get("lane");
    assertEquals("Batch should contain 10 records", maxBatchSize, parsedRecords.size());

    for (int j = 0; j < maxBatchSize; j++) {
      Record rec = parsedRecords.get(j);
      assertEquals(rec.get("/_id").getValueAsString(), keys[j]);
      assertEquals(rec.get("/aa").getValueAsString(), "hello");
      assertEquals(rec.get("/bb").getValueAsString(), "world");
      assertEquals(rec.get("/num").getValueAsLong(), 12345);
    }

  }

  private void insertRows(String json, int numDocs, boolean binary) {
    Table tab = createTable();

    try {
      for (int i = 0; i < numDocs; i++) {
        tab.insert(createDocument(json, i, binary));
      }
      tab.flush();
      tab.close();
    } catch (DBException ex) {
      throw ex;
    }

  }

  private Table createTable() {
    try {
      return MapRDB.createTable(TABLE_NAME);
    } catch (DBException ex) {
      throw ex;
    }

  }

  private Document createDocument(String json, int key, boolean binary) {
    Document doc = MapRDB.newDocument(json);
    if(binary) {
      byte[] barr = Bytes.toBytes(key);
      doc.setId(ByteBuffer.wrap(barr));
    } else {
      doc.setId("" + key);
    }
    return doc;
  }

}
