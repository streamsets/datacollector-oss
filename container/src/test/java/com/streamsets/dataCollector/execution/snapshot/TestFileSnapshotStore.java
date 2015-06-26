/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.snapshot;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.dataCollector.execution.SnapshotInfo;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.runner.ErrorSink;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.util.PipelineException;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestFileSnapshotStore {

  private static final String TEST_STRING = "TestFileSnapshotStore";
  private static final String MIME = "application/octet-stream";
  private static final String SNAPSHOT_ID = "mySnapshotId";
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "0";
  private static final String USER = "user";

  private FileSnapshotStore snapshotStore = null;

  @Module(injects = FileSnapshotStore.class, library = true)
  public static class TestSnapshotStoreModule {

    @Provides
    public RuntimeInfo provideRuntimeInfo() {
      return new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        ImmutableList.of(getClass().getClassLoader()));
    }
  }

  @BeforeClass
  public static void beforeClass() {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
  }

  @AfterClass
  public static void afterClass() {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Before
  public void setUp() throws IOException {
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    ObjectGraph objectGraph = ObjectGraph.create(TestSnapshotStoreModule.class);
    snapshotStore = new FileSnapshotStore();
    objectGraph.inject(snapshotStore);
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testCreate() throws PipelineRuntimeException {
    long before = System.currentTimeMillis();
    SnapshotInfo snapshotInfo = snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);
    long after = System.currentTimeMillis();
    Assert.assertNotNull(snapshotInfo);
    Assert.assertEquals(SNAPSHOT_ID, snapshotInfo.getId());
    Assert.assertEquals(PIPELINE_NAME, snapshotInfo.getName());
    Assert.assertEquals(PIPELINE_REV, snapshotInfo.getRev());
    Assert.assertEquals(USER, snapshotInfo.getUser());
    Assert.assertTrue(snapshotInfo.getTimeStamp() >= before && snapshotInfo.getTimeStamp() <= after);
    Assert.assertTrue(snapshotInfo.isInProgress());
  }

  @Test(expected = PipelineRuntimeException.class)
  public void testSaveBeforeCreate() throws PipelineException {
    snapshotStore.save(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, new ArrayList<List<StageOutput>>());
  }

  @Test
  public void testGetWithoutSave() throws PipelineException {

    InputStream inputStream = snapshotStore.getData(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);
    Assert.assertNull(inputStream);

    //create snapshot - has snapshot info with "inProgress" but no data
    snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);

    inputStream = snapshotStore.getData(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);
    Assert.assertNull(inputStream);

    SnapshotInfo snapshotInfo = snapshotStore.getInfo(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);
    Assert.assertEquals(SNAPSHOT_ID, snapshotInfo.getId());
    Assert.assertEquals(PIPELINE_NAME, snapshotInfo.getName());
    Assert.assertEquals(PIPELINE_REV, snapshotInfo.getRev());
    Assert.assertEquals(USER, snapshotInfo.getUser());
    Assert.assertTrue(snapshotInfo.isInProgress());
  }

  @Test
  public void testSaveAndGet() throws PipelineException {
    SnapshotInfo snapshotInfo = snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);
    Assert.assertTrue(snapshotInfo.isInProgress());

    snapshotStore.save(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID, getSnapshotData());
    snapshotInfo =  snapshotStore.getInfo(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);
    Assert.assertEquals(SNAPSHOT_ID, snapshotInfo.getId());
    Assert.assertEquals(PIPELINE_NAME, snapshotInfo.getName());
    Assert.assertEquals(PIPELINE_REV, snapshotInfo.getRev());
    Assert.assertEquals(USER, snapshotInfo.getUser());
    Assert.assertFalse(snapshotInfo.isInProgress());

    InputStream data = snapshotStore.getData(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);
    Assert.assertNotNull(data);
  }

  @Test
  public void testGetSummary() throws PipelineException {
    List<SnapshotInfo> summaryForPipeline = snapshotStore.getSummaryForPipeline(PIPELINE_NAME, PIPELINE_REV);
    Assert.assertNotNull(summaryForPipeline);
    Assert.assertEquals(0, summaryForPipeline.size());

    for(int i = 0; i < 3; i++) {
      snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + i);
      snapshotStore.save(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + i, getSnapshotData());
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
  public void testDelete() throws PipelineException {
    //NO-OP since no snapshot exists
    snapshotStore.deleteSnapshot(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID);

    //create and save
    snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 0);
    snapshotStore.save(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 0, getSnapshotData());
    snapshotStore.create(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 1);
    snapshotStore.save(USER, PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 1, getSnapshotData());
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
    InputStream data = snapshotStore.getData(PIPELINE_NAME, PIPELINE_REV, SNAPSHOT_ID + 1);
    Assert.assertNull(data);

  }

  private List<List<StageOutput>> getSnapshotData() {
    List<List<StageOutput>> snapshotBatches = new ArrayList<>();
    snapshotBatches.add(createSnapshotData());
    snapshotBatches.add(createSnapshotData());
    return snapshotBatches;
  }

  private List<StageOutput> createSnapshotData() {

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

    StageOutput s1 = new StageOutput("source", so1, new ErrorSink());
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
    StageOutput s2 = new StageOutput("processor", so2, new ErrorSink());
    snapshot.add(s2);

    return snapshot;
  }
}
