/**
 * Licensed to the Apache Software Foundation (ASF) under one
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

package com.streamsets.pipeline.errorrecordstore.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.record.RecordImpl;
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
  public static void beforeClass() {
    System.setProperty("pipeline.data.dir", "./target/var");
  }

  @AfterClass
  public static void afterClass() {
    System.getProperties().remove("pipeline.data.dir");
  }

  @Before
  public void setUp() throws IOException {
    File f = new File(System.getProperty("pipeline.data.dir"));
    FileUtils.deleteDirectory(f);
    RuntimeInfo info = new RuntimeInfo(ImmutableList.of(getClass().getClassLoader()));
    errorStore = new FileErrorRecordStore(info);
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testStoreErrorRecord() {
    Assert.assertTrue(errorStore.getErrorRecords(PIPELINE_NAME, REV, SOURCE_NAME) == null);
    Assert.assertTrue(errorStore.getErrorRecords(PIPELINE_NAME, REV, PROCESSOR_NAME) == null);

    errorStore.storeErrorRecords(PIPELINE_NAME, REV, createErrorRecordData());

    InputStream in = errorStore.getErrorRecords(PIPELINE_NAME, REV, SOURCE_NAME);
    Assert.assertNotNull(in);
    in = errorStore.getErrorRecords(PIPELINE_NAME, REV, PROCESSOR_NAME);
    Assert.assertNotNull(in);

    //TODO: Retrieve error records and compare contents once de-serializer is ready

  }

  @Test(expected = RuntimeException.class)
  public void testStoreInvalidDir() {
    RuntimeInfo info = Mockito.mock(RuntimeInfo.class);
    Mockito.when(info.getDataDir()).thenReturn("\0");
    errorStore = new FileErrorRecordStore(info);

    //Runtime exception expected
    errorStore.storeErrorRecords(PIPELINE_NAME, REV, createErrorRecordData());
  }

  @Test
  public void testGetErrorRecordsWhenItDoesNotExist() {
    Assert.assertNull(errorStore.getErrorRecords("someArbitraryPipeline", REV, SOURCE_NAME));
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
}