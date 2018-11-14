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
package com.streamsets.datacollector.execution.snapshot;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.ErrorSink;
import com.streamsets.datacollector.runner.EventSink;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class TestSnapshotStore {

  private static final String TEST_STRING = "TestSnapshotStore";
  private static final String MIME = "application/octet-stream";
  private static final String SNAPSHOT_ID = "mySnapshotId";
  private static final String SNAPSHOT_LABEL = "mySnapshotLabel";
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "0";
  private static final String USER = "user";

  protected SnapshotStore snapshotStore = null;

  @Test
  public void testCreate() throws PipelineException {
    long before = System.currentTimeMillis();
    SnapshotInfo snapshotInfo = snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, SNAPSHOT_LABEL, false);
    long after = System.currentTimeMillis();
    Assert.assertNotNull(snapshotInfo);
    Assert.assertEquals(SNAPSHOT_ID, snapshotInfo.getId());
    Assert.assertEquals(SNAPSHOT_LABEL, snapshotInfo.getLabel());
    Assert.assertEquals(PIPELINE_NAME, snapshotInfo.getName());
    Assert.assertEquals(PIPELINE_REV, snapshotInfo.getRev());
    Assert.assertEquals(USER, snapshotInfo.getUser());
    Assert.assertTrue(snapshotInfo.getTimeStamp() >= before && snapshotInfo.getTimeStamp() <= after);
    Assert.assertTrue(snapshotInfo.isInProgress());
  }

  @Test(expected = PipelineException.class)
  public void testCreateWhileSnapshotExists() throws PipelineException {
    // Create snapshot
    SnapshotInfo snapshotInfo = snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, SNAPSHOT_LABEL, false);
    Assert.assertNotNull(snapshotInfo);

    // Try to create it again
    snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, SNAPSHOT_LABEL, false);
  }

  @Test
  public void testUpdateLabel() throws PipelineException {
    SnapshotInfo snapshotInfo = snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, SNAPSHOT_LABEL, false);
    Assert.assertNotNull(snapshotInfo);
    Assert.assertEquals(SNAPSHOT_ID, snapshotInfo.getId());
    Assert.assertEquals(SNAPSHOT_LABEL, snapshotInfo.getLabel());
    Assert.assertEquals(PIPELINE_NAME, snapshotInfo.getName());
    Assert.assertEquals(PIPELINE_REV, snapshotInfo.getRev());
    Assert.assertEquals(USER, snapshotInfo.getUser());
    Assert.assertTrue(snapshotInfo.isInProgress());

    SnapshotInfo updatedSnapshotInfo = snapshotStore.updateLabel(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID,
        "Snapshot data for debug");
    Assert.assertNotNull(updatedSnapshotInfo);
    Assert.assertEquals(SNAPSHOT_ID, updatedSnapshotInfo.getId());
    Assert.assertEquals("Snapshot data for debug", updatedSnapshotInfo.getLabel());
    Assert.assertEquals(PIPELINE_NAME, updatedSnapshotInfo.getName());
    Assert.assertEquals(PIPELINE_REV, updatedSnapshotInfo.getRev());
    Assert.assertEquals(USER, updatedSnapshotInfo.getUser());
    Assert.assertTrue(updatedSnapshotInfo.isInProgress());
  }


  @Test(expected = PipelineException.class)
  public void testSaveBeforeCreate() throws PipelineException {
    snapshotStore.save(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, 0, new ArrayList<List<StageOutput>>());
  }

  @Test
  public void testGetWithoutSave() throws PipelineException {

    InputStream inputStream = snapshotStore.get(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID).getOutput();
    Assert.assertNull(inputStream);

    //create snapshot - has snapshot info with "inProgress" but no data
    snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, SNAPSHOT_LABEL, false);

    inputStream = snapshotStore.get(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID).getOutput();
    Assert.assertNull(inputStream);

    SnapshotInfo snapshotInfo = snapshotStore.getInfo(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);
    Assert.assertEquals(SNAPSHOT_ID, snapshotInfo.getId());
    Assert.assertEquals(PIPELINE_NAME, snapshotInfo.getName());
    Assert.assertEquals(PIPELINE_REV, snapshotInfo.getRev());
    Assert.assertEquals(USER, snapshotInfo.getUser());
    Assert.assertTrue(snapshotInfo.isInProgress());
  }

  @Test
  public void testSaveAndGet() throws Exception {
    SnapshotInfo snapshotInfo = snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, SNAPSHOT_LABEL, false);
    Assert.assertTrue(snapshotInfo.isInProgress());

    snapshotStore.save(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, 0, getSnapshotData());
    snapshotInfo =  snapshotStore.getInfo(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);
    Assert.assertEquals(SNAPSHOT_ID, snapshotInfo.getId());
    Assert.assertEquals(PIPELINE_NAME, snapshotInfo.getName());
    Assert.assertEquals(PIPELINE_REV, snapshotInfo.getRev());
    Assert.assertEquals(USER, snapshotInfo.getUser());
    Assert.assertFalse(snapshotInfo.isInProgress());

    InputStream data = snapshotStore.get(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID).getOutput();
    Assert.assertNotNull(data);
  }

  @Test
  public void testGetSummary() throws Exception {
    List<SnapshotInfo> summaryForPipeline = snapshotStore.getSummaryForPipeline(PIPELINE_NAME, PIPELINE_REV);
    Assert.assertNotNull(summaryForPipeline);
    Assert.assertEquals(0, summaryForPipeline.size());

    for(int i = 0; i < 3; i++) {
      snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + i, SNAPSHOT_LABEL, false);
      snapshotStore.save(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + i, 0, getSnapshotData());
    }

    summaryForPipeline = snapshotStore.getSummaryForPipeline(PIPELINE_NAME, PIPELINE_REV);
    Assert.assertNotNull(summaryForPipeline);
    Assert.assertEquals(3, summaryForPipeline.size());

    Set<String> actualIds = new HashSet<>(3);

    for(int i = 0; i < 3; i++) {
      SnapshotInfo snapshotInfo = summaryForPipeline.get(i);
      //Cannot guarantee order of summary, so add it to set.
      actualIds.add(snapshotInfo.getId());
      Assert.assertEquals(PIPELINE_NAME, snapshotInfo.getName());
      Assert.assertEquals(PIPELINE_REV, snapshotInfo.getRev());
      Assert.assertEquals(USER, snapshotInfo.getUser());
      Assert.assertFalse(snapshotInfo.isInProgress());
    }

    for(int i = 0; i < 3; i++) {
      Assert.assertTrue(actualIds.contains(SNAPSHOT_ID + i));
    }
  }

  @Test
  public void testDelete() throws Exception {
    //NO-OP since no snapshot exists
    snapshotStore.deleteSnapshot(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);

    //create and save
    snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 0, SNAPSHOT_LABEL + 0, false);
    snapshotStore.save(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 0, 0, getSnapshotData());
    snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 1, SNAPSHOT_LABEL + 1, false);
    snapshotStore.save(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 1, 0, getSnapshotData());
    List<SnapshotInfo> summaryForPipeline = snapshotStore.getSummaryForPipeline(PIPELINE_NAME, PIPELINE_REV);
    Assert.assertEquals(2, summaryForPipeline.size());

    //delete one snapshot
    snapshotStore.deleteSnapshot(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 0);
    summaryForPipeline = snapshotStore.getSummaryForPipeline(PIPELINE_NAME, PIPELINE_REV);
    Assert.assertEquals(1, summaryForPipeline.size());

    //delete random snapshot which does nto exist
    snapshotStore.deleteSnapshot(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + "random");
    summaryForPipeline = snapshotStore.getSummaryForPipeline(PIPELINE_NAME, PIPELINE_REV);
    Assert.assertEquals(1, summaryForPipeline.size());

    //delete the other snapshot
    snapshotStore.deleteSnapshot(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 1);
    summaryForPipeline = snapshotStore.getSummaryForPipeline(PIPELINE_NAME, PIPELINE_REV);
    Assert.assertEquals(0, summaryForPipeline.size());

    SnapshotInfo snapshotInfo = snapshotStore.getInfo(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 1);
    Assert.assertNull(snapshotInfo);
    InputStream data = snapshotStore.get(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 1).getOutput();
    Assert.assertNull(data);

  }

  @Test
  public void testSnapshotClose() throws Exception {
    SnapshotInfo snapshotInfo = snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, SNAPSHOT_LABEL, false);
    Assert.assertTrue(snapshotInfo.isInProgress());

    snapshotStore.save(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, 0, getSnapshotData());

    Snapshot snapshot = snapshotStore.get(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);
    Assert.assertNotNull(snapshot.getInfo());
    Assert.assertNotNull(snapshot.getOutput());

    snapshot.close();
    snapshot.close();

    Assert.assertNull(snapshot.getInfo());
    Assert.assertNull(snapshot.getOutput());

  }

  private List<List<StageOutput>> getSnapshotData() throws Exception {
    List<List<StageOutput>> snapshotBatches = new ArrayList<>();
    snapshotBatches.add(createSnapshotData());
    snapshotBatches.add(createSnapshotData());
    return snapshotBatches;
  }

  private List<StageOutput> createSnapshotData() throws Exception  {
    ErrorSink errorSink = new ErrorSink();
    EventSink eventSink = new EventSink();
    ImmutableList.of("source", "processor").forEach(instanceName -> {
      errorSink.registerInterceptorsForStage(instanceName, Collections.emptyList());
      eventSink.registerInterceptorsForStage(instanceName, Collections.emptyList());
    });

    List<StageOutput> snapshot = new ArrayList<>(2);

    List<Record> records1 = new ArrayList<>(2);

    Record r1 = new RecordImpl("s", "s:1", TEST_STRING.getBytes(), MIME);
    r1.set(Field.create(1));

    ((RecordImpl)r1).createTrackingId();
    ((RecordImpl)r1).createTrackingId();

    Record r2 = new RecordImpl("s", "s:2", TEST_STRING.getBytes(), MIME);
    r2.set(Field.create(2));

    ((RecordImpl)r2).createTrackingId();
    ((RecordImpl)r2).createTrackingId();

    records1.add(r1);
    records1.add(r2);

    Map<String, List<Record>> so1 = new HashMap<>(1);
    so1.put("lane", records1);

    StageOutput s1 = new StageOutput("source", so1, errorSink, eventSink);
    snapshot.add(s1);

    List<Record> records2 = new ArrayList<>(1);
    Record r3 = new RecordImpl("s", "s:3", TEST_STRING.getBytes(), MIME);
    r3.set(Field.create(1));

    ((RecordImpl)r3).createTrackingId();
    ((RecordImpl)r3).createTrackingId();

    Record r4 = new RecordImpl("s", "s:2", TEST_STRING.getBytes(), MIME);
    r4.set(Field.create(2));

    ((RecordImpl)r4).createTrackingId();
    ((RecordImpl)r4).createTrackingId();

    records2.add(r3);

    Map<String, List<Record>> so2 = new HashMap<>(1);
    so2.put("lane", records2);
    StageOutput s2 = new StageOutput("processor", so2, errorSink, eventSink);
    snapshot.add(s2);

    return snapshot;
  }

}
