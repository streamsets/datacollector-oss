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
package com.streamsets.pipeline.lib.basics;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.basics.log.TailLogSource;
import com.streamsets.pipeline.sdk.testharness.SourceRunner;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestTailLogSource {

  private String logFile = "logFile.txt";
  private static final String SOURCE_LOG_FILE_RESOURCE = "testLogFile.txt";
  private LogGenerator logGen;

  @Before
  public void setUp() throws IOException {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    testDataDir.mkdirs();
    logFile = new File(testDataDir, "logFile.txt").getAbsolutePath();
    //spawn a thread that starts writing to the file from which the
    //TailLogSource will read lines
    Files.createFile(Paths.get(logFile));
    assert(Files.exists(Paths.get(logFile)));
    logGen = new LogGenerator(SOURCE_LOG_FILE_RESOURCE, logFile);
    this.logGen.start();
    logGen.waitUntilFileCreation();
  }

  @After
  public void tearDown() {
    //wait for the log generator thread to complete writing
    try {
      logGen.interrupt();
      logGen.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    //remove the file in the target directory
    try {
      Files.deleteIfExists(Paths.get(logFile));
    } catch (IOException e) {
      e.printStackTrace();
    }
    assert(!Files.exists(Paths.get(logFile)));
  }

  @Test
  public void testTailLogSourceAllOptions() {
    try {
      logGen.startGeneratingLog();
      Map<String, List<Record>> result = new SourceRunner.Builder<TailLogSource>().addSource(TailLogSource.class)
        .configure("logFileName", logFile)
        .configure("tailFromEnd", true)
        .configure("maxLinesPrefetch", 50)
        .configure("batchSize", 25)
        .configure("maxWaitTime", 1000)
        .configure("logLineRecordFieldName", "logLine")
        .maxBatchSize(100)
        .outputLanes(ImmutableSet.of("lane"))
        .sourceOffset(null)
        .build()
        .run();

      Assert.assertNotNull(result);
      Assert.assertNotNull(result.get("lane"));
      //Expected 25 records as the "batchSize" property is set to 25
      Assert.assertEquals(25, result.get("lane").size());

      printRecords(result.get("lane"));

    } catch (StageException e) {
      Assert.fail("Unexpected exception. Cause: " + e.getCause() );
    }
  }

  @Test
  public void testTailLogSourceFromBegining() {
    try {
      logGen.startGeneratingLog();
      Map<String, List<Record>> result = new SourceRunner.Builder<TailLogSource>().addSource(TailLogSource.class)
        .configure("logFileName", logFile)
        .configure("tailFromEnd", false)
        .configure("maxLinesPrefetch", 50)
        .configure("batchSize", 25)
        .configure("maxWaitTime", 1000)
        .configure("logLineRecordFieldName", "logLine")
        .maxBatchSize(100)
        .outputLanes(ImmutableSet.of("lane"))
        .sourceOffset(null)
        .build()
        .run();

      //TODO
      /*Assert.assertNotNull(result);
      Assert.assertNotNull(result.get("lane"));
      //Expected 25 records as the "batchSize" property is set to 25
      Assert.assertTrue(result.get("lane").size() == 25);*/

      printRecords(result.get("lane"));

    } catch (StageException e) {
      Assert.fail("Unexpected exception. Cause: " + e.getCause() );
    }
  }

  private void printRecords(List<Record> records) {
    System.out.println("The following records were produced");
    System.out.println("***********************************");
    for(Record r : records) {
      System.out.println(r.toString());
    }
  }

  @Test
  public void testTailLogSourceDefaultPipelineOptions() {
    try {
      logGen.startGeneratingLog();
      Map<String, List<Record>> result = new SourceRunner.Builder<TailLogSource>().addSource(TailLogSource.class)
        .configure("logFileName", logFile)
        .configure("tailFromEnd", true)
        .configure("maxLinesPrefetch", 50)
        .configure("batchSize", 25)
        .configure("maxWaitTime", 1000)
        .configure("logLineRecordFieldName", "logLine")
        .build()
        .run();

      Assert.assertNotNull(result);
      Assert.assertNotNull(result.get("lane"));
      //10 records expected as the default pipeline maxBatchSize is not
      //set and the default is used which is 10
      //The minimum of the maxBatchSize and the "batchSize" is expected
      Assert.assertTrue(result.get("lane").size() == 10);

      printRecords(result.get("lane"));

    } catch (StageException e) {
      Assert.fail("Unexpected exception. Cause: " + e.getCause());
    }
  }

  @Test
  public void testTailLogSourceNoConfig() {
    try {
      logGen.startGeneratingLog();
      Map<String, List<Record>> result = new SourceRunner.Builder<TailLogSource>().addSource(TailLogSource.class)
        .configure("logFileName", logFile)
        .build()
        .run();

      Assert.assertNotNull(result);
      Assert.assertNotNull(result.get("lane"));
      //10 records [default] expected as neither batchSize config nor the the
      // pipeline maxBatchSize is set.
      Assert.assertTrue(result.get("lane").size() == 10);

      printRecords(result.get("lane"));

    } catch (StageException e) {
      Assert.fail("Unexpected exception. Cause: " + e.getCause());
    }
  }

  @Test
  public void testTailLogSourceNoFile() {
    try {
      Map<String, List<Record>> result = new SourceRunner.Builder<TailLogSource>().addSource(TailLogSource.class)
        .configure("logFileName", "fileDoesNotExist.txt")
        .build()
        .run();

      Assert.assertNotNull(result);
      Assert.assertNull(result.get("lane"));

    } catch (StageException e) {
      Assert.fail("Unexpected exception. Cause: " + e.getCause());
    }
  }

  @Test
  public void testTailLogSourceWrongOptions() {
    try {
      logGen.startGeneratingLog();
      Map<String, List<Record>> result = new SourceRunner.Builder<TailLogSource>()
        .addSource(TailLogSource.class)
        .configure("logFileName", 50)
        .configure("tailFromEnd", "true")
        .configure("maxLinesPrefetch", 50.50)
        .configure("batchSize", "twenty five")
        .configure("maxWaitTime", "two thousand")
        .configure("logLineRecordFieldName", 10)
        .build()
        .run();

      Assert.fail("IllegalStateException expected as wrong configuration values are supplied.");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains(
        "SourceBuilder is not configured correctly. Please check the logs for errors"));
    } catch (StageException e) {
      Assert.fail("Unexpected exception. Cause: " + e.getCause() );
    }
  }
}
