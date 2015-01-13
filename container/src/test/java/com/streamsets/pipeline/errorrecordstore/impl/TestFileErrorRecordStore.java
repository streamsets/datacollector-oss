/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.pipeline.errorrecordstore.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.LogUtil;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestFileErrorRecordStore {

  private static final String TEST_STRING = "TestFileErrorRecordStore";
  private static final String MIME = "application/octet-stream";
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String SOURCE_NAME = "mySource";
  private static final String PROCESSOR_NAME = "myProc";
  private static final String REV = "0";

  private FileErrorRecordStore errorStore = null;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeInfo.DATA_DIR, "./target/var");
  }

  @AfterClass
  public static void afterClass() {
    System.getProperties().remove(RuntimeInfo.DATA_DIR);
  }

  @Before
  public void setUp() throws IOException {
    File f = new File(System.getProperty(RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    RuntimeInfo info = new RuntimeInfo(ImmutableList.of(getClass().getClassLoader()));
    Configuration configuration = new Configuration();
    errorStore = new FileErrorRecordStore(info, configuration);
  }

  @After
  public void tearDown() {
    LogUtil.unregisterAllLoggers();
  }

  @Test
  public void testStoreErrorRecord() {
    //The rolling file appender opens the file on initialization.
    Assert.assertTrue(errorStore.getErrors(PIPELINE_NAME, REV) == null);
    errorStore.register(PIPELINE_NAME, REV);
    errorStore.storeErrorRecords(PIPELINE_NAME, REV, createErrorRecordData());
    InputStream in = errorStore.getErrors(PIPELINE_NAME, REV);
    Assert.assertNotNull(in);
    //TODO: Retrieve error records and compare contents once de-serializer is ready
  }

  @Test
  public void testStoreErrorMessages() throws IOException {
    //The rolling file appender opens the file on initialization.
    Assert.assertTrue(errorStore.getErrors(PIPELINE_NAME, REV) == null);

    errorStore.register(PIPELINE_NAME, REV);
    errorStore.storeErrorMessages(PIPELINE_NAME, REV, createErrorMessages());

    InputStream in = errorStore.getErrors(PIPELINE_NAME, REV);
    Assert.assertNotNull(in);

    Map map = ObjectMapperFactory.get().readValue(in, Map.class);
    Assert.assertTrue(map != null && !map.isEmpty());
    Assert.assertTrue(map.containsKey("stage"));
    Assert.assertEquals(PROCESSOR_NAME, map.get("stage"));
    Assert.assertTrue(map.containsKey("type"));
    Assert.assertEquals("pipeline", map.get("type"));
    Assert.assertTrue(map.containsKey("error"));

  }

  @Test(expected = RuntimeException.class)
  public void testStoreInvalidDir() {
    RuntimeInfo info = Mockito.mock(RuntimeInfo.class);
    Mockito.when(info.getDataDir()).thenReturn("\0");
    errorStore = new FileErrorRecordStore(info, new Configuration());

    //Runtime exception expected
    errorStore.register(PIPELINE_NAME, REV);
    errorStore.storeErrorRecords(PIPELINE_NAME, REV, createErrorRecordData());
  }

  @Test
  public void testGetErrorRecordsWhenItDoesNotExist() {
    Assert.assertNull(errorStore.getErrors("someArbitraryPipeline", REV));
  }

  private Map<String, List<Record>> createErrorRecordData() {

    Map<String, List<Record>> errorRecords = new HashMap<>();

    Record r1 = new RecordImpl("s", "s:1", TEST_STRING.getBytes(), MIME);

    r1.set(Field.create(1));

    ((RecordImpl)r1).getHeader().setTrackingId("t1");

    Record r2 = new RecordImpl("s", "s:2", TEST_STRING.getBytes(), MIME);
    r2.set(Field.create(2));

    ((RecordImpl)r2).getHeader().setTrackingId("t2");

    Record r3 = new RecordImpl("s", "s:3", TEST_STRING.getBytes(), MIME);
    r3.set(Field.create(1));

    ((RecordImpl)r3).getHeader().setTrackingId("t3");

    Record r4 = new RecordImpl("s", "s:2", TEST_STRING.getBytes(), MIME);
    r4.set(Field.create(2));

    ((RecordImpl)r4).getHeader().setTrackingId("t4");

    List<Record> sourceErrorRecords = new ArrayList<>();
    sourceErrorRecords.add(r1);
    sourceErrorRecords.add(r2);

    List<Record> procErrorRecords = new ArrayList<>();
    sourceErrorRecords.add(r3);
    sourceErrorRecords.add(r4);

    errorRecords.put(SOURCE_NAME, sourceErrorRecords);
    errorRecords.put(PROCESSOR_NAME, procErrorRecords);
    return errorRecords;
  }

  private Map<String, List<ErrorMessage>> createErrorMessages() {
    Map<String, List<ErrorMessage>> errorMessages = new HashMap<>();

    List<ErrorMessage> errorMessageList = new ArrayList<>();
    errorMessageList.add(new ErrorMessage("a", "b", 2L));

    errorMessages.put(PROCESSOR_NAME, errorMessageList);
    return errorMessages;
  }

}