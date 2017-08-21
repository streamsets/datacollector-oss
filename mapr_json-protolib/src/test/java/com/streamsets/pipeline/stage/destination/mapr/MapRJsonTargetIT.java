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
package com.streamsets.pipeline.stage.destination.mapr;

import com.google.common.collect.ImmutableList;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.exceptions.DBException;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.ojai.Document;
import org.ojai.DocumentStream;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

@Category(SingleForkNoReuseTest.class)
@Ignore
public class MapRJsonTargetIT {

  private static final String TABLE_NAME = "mytable";

@After
public void deleteTable() {
  try {
    MapRDB.deleteTable(TABLE_NAME);
  } catch (DBException ex) {
    throw ex;
  }
}

private void basicConfiguration(MapRJsonConfigBean conf) {

    conf.tableName = TABLE_NAME;
    conf.createTable = false;
    conf.isBinaryRowKey = false;
    conf.keyField = "/rowKey";
    conf.insertOrReplace = InsertOrReplace.INSERT;

  }

  @Test(timeout = 10000)
  public void testInsertADocument() throws Exception {

    MapRJsonConfigBean conf = new MapRJsonConfigBean();
    basicConfiguration(conf);
    conf.createTable = true;

    MapRJsonTarget target = new MapRJsonTarget(conf);

    TargetRunner runner = new TargetRunner.Builder(MapRJsonDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    List<Record> singleRecord = ImmutableList.of(newRecord(27, 0));

    runner.runInit();
    runner.runWrite(singleRecord);

    assertEquals(1, recordCount());
    assertNotNull(fetchDocument());

  }

  @Test(timeout = 10000)
  public void testBinaryKeyInsertADocument() throws Exception {

    MapRJsonConfigBean conf = new MapRJsonConfigBean();
    basicConfiguration(conf);
    conf.createTable = true;
    conf.isBinaryRowKey = true;

    MapRJsonTarget target = new MapRJsonTarget(conf);

    TargetRunner runner = new TargetRunner.Builder(MapRJsonDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    List<Record> singleRecord = ImmutableList.of(newRecord(27, 0));

    runner.runInit();
    runner.runWrite(singleRecord);

    assertEquals(1, recordCount());
    assertNotNull(fetchDocument());

  }

  @Test(timeout = 10000)
  public void testBinaryKeyInsertDuplicate() throws Exception {

    MapRJsonConfigBean conf = new MapRJsonConfigBean();
    basicConfiguration(conf);

    conf.createTable = true;
    conf.insertOrReplace = InsertOrReplace.INSERT;
    conf.isBinaryRowKey = true;

    MapRJsonTarget target = new MapRJsonTarget(conf);

    TargetRunner runner = new TargetRunner.Builder(MapRJsonDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    List<Record> twoRecords = ImmutableList.of(newRecord(27, 0), newRecord(27, 1));

    runner.runInit();
    runner.runWrite(twoRecords);

    assertEquals(1, recordCount());
    assertNotNull(fetchDocument());

  }

  @Test(timeout = 10000)
  public void testInsertDuplicate() throws Exception {

    MapRJsonConfigBean conf = new MapRJsonConfigBean();
    basicConfiguration(conf);

    conf.createTable = true;
    conf.insertOrReplace = InsertOrReplace.INSERT;

    MapRJsonTarget target = new MapRJsonTarget(conf);

    TargetRunner runner = new TargetRunner.Builder(MapRJsonDTarget.class,
        target
    ).setOnRecordError(OnRecordError.TO_ERROR).build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    List<Record> twoRecords = ImmutableList.of(newRecord(27, 0), newRecord(27, 1));

    runner.runInit();
    runner.runWrite(twoRecords);

    assertEquals(1, recordCount());
    assertNotNull(fetchDocument());

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals(errors.get(0).get("/rowKey").getValueAsInteger(), 27);
    assertEquals(errors.get(0).get("/a").getValueAsInteger(), 21);

  }

  @Test(timeout = 10000)
  public void testInsertDuplicateBinaryKey() throws Exception {

    MapRJsonConfigBean conf = new MapRJsonConfigBean();
    basicConfiguration(conf);
    conf.createTable = true;
    conf.insertOrReplace = InsertOrReplace.INSERT;
    conf.isBinaryRowKey = true;

    MapRJsonTarget target = new MapRJsonTarget(conf);

    TargetRunner runner = new TargetRunner.Builder(MapRJsonDTarget.class,
        target
    ).setOnRecordError(OnRecordError.TO_ERROR).build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    List<Record> twoRecords = ImmutableList.of(newRecord(27, 0), newRecord(27, 1));

    runner.runInit();
    runner.runWrite(twoRecords);

    assertEquals(1, recordCount());
    assertNotNull(fetchDocument());

    // one, since inserting with existing record is an error.
    assertEquals(1, runner.getErrorRecords().size());


  }

  @Test(timeout = 10000)
  public void testInsertOrReplaceDuplicate() throws Exception {

    MapRJsonConfigBean conf = new MapRJsonConfigBean();
    basicConfiguration(conf);

    conf.createTable = true;
    conf.insertOrReplace = InsertOrReplace.REPLACE;

    MapRJsonTarget target = new MapRJsonTarget(conf);

    TargetRunner runner = new TargetRunner.Builder(MapRJsonDTarget.class,
        target
    ).setOnRecordError(OnRecordError.TO_ERROR).build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    List<Record> twoRecords = ImmutableList.of(newRecord(27,0), newRecord(27, 1));

    runner.runInit();
    runner.runWrite(twoRecords);

    // one, since the record was replaced.
    assertEquals(1, recordCount());
    assertNotNull(fetchDocument());

    // zero, since replacing a record is not an error.
    assertEquals(0, runner.getErrorRecords().size());

  }

  @Test(timeout = 10000)
  public void testInsertOrReplaceDuplicateBinaryKey() throws Exception {

    MapRJsonConfigBean conf = new MapRJsonConfigBean();
    basicConfiguration(conf);
    conf.createTable = true;
    conf.insertOrReplace = InsertOrReplace.REPLACE;
    conf.isBinaryRowKey = true;

    MapRJsonTarget target = new MapRJsonTarget(conf);

    TargetRunner runner = new TargetRunner.Builder(MapRJsonDTarget.class,
        target
    ).setOnRecordError(OnRecordError.DISCARD).build();

    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    assertEquals(0, configIssues.size());

    List<Record> twoRecords = ImmutableList.of(newRecord(27,0), newRecord(27, 1));

    runner.runInit();
    runner.runWrite(twoRecords);

    assertEquals(1, recordCount());
    assertNotNull(fetchDocument());

  }

  private Record newRecord(int rowId, int val) {

    Map<String, Field> r1 = new LinkedHashMap<>();
    r1.put("a", Field.create(20 + val));
    r1.put("rowKey", Field.create(rowId));
    r1.put("b", Field.create(30 + val));
    r1.put("c", Field.create(40 + val));
    r1.put("d", Field.create(50 + val));

    Record rec = RecordCreator.create("s", "s:1");
    rec.set(Field.create(r1));

    return rec;
  }

  private Document fetchDocument() {
    Table tab;
    try {
      tab = MapRDB.getTable(TABLE_NAME);
      DocumentStream stream = tab.find();
      Iterator<Document> iter = stream.iterator();
      Document doc = iter.next();
      tab.close();
      return doc;

    } catch (DBException ex) {
      throw ex;
    }
  }

  private int recordCount() {
    Table tab;
    try {
      tab = MapRDB.getTable(TABLE_NAME);
      DocumentStream stream = tab.find();
      Iterator<Document> iter = stream.iterator();
      int i = 0 ;
      while(iter.hasNext()) {
        iter.next();    // don't care about the returned Document.
        i++;
      }
      tab.close();
      return i;

    } catch (DBException ex) {
      throw ex;
    }
  }

}
