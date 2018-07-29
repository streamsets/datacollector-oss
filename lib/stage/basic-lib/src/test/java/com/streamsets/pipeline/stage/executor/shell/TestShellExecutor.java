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
package com.streamsets.pipeline.stage.executor.shell;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.executor.shell.config.ShellConfig;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestShellExecutor {

  private ShellConfig config;
  private File handoverFile;

  @Before
  public void setUp() throws Exception {
    this.config = new ShellConfig();
    this.handoverFile = File.createTempFile("handover-test", ".txt");
  }

  @After
  public void cleanUp() throws Exception {
    if(handoverFile != null && handoverFile.exists()) {
      handoverFile.delete();
    }
  }

  /**
   * Validate that given file exists and have expected content.
   */
  private void assertHandoverFile(String content) throws Exception {
    Assert.assertTrue("Output file doesn't exists: " + handoverFile.getPath(), handoverFile.exists());
    try(InputStream stream = new FileInputStream(handoverFile)) {
      StringWriter writer = new StringWriter();
      IOUtils.copy(stream, writer, "UTF-8");

      assertEquals(content, writer.toString());
    }
  }

  /**
   * We have one test record for all operations
   */
  private Record getTestRecord() {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.MAP, ImmutableMap.builder()
      .put("company", Field.create(Field.Type.STRING, "StreamSets"))
      .build()
    ));
    return record;
  }

  @Test
  public void testSimpleInvocation() throws Exception {
    config.script = "echo 'a' > " + handoverFile.getPath();

    ShellExecutor executor = new ShellExecutor(config);

    ExecutorRunner runner = new ExecutorRunner.Builder(ShellDExecutor.class, executor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(getTestRecord()));
    runner.runDestroy();

    assertHandoverFile("a\n");
  }

  @Test
  public void testSMultipleRecords() throws Exception {
    config.script = "echo 'a' >> " + handoverFile.getPath();

    ShellExecutor executor = new ShellExecutor(config);

    ExecutorRunner runner = new ExecutorRunner.Builder(ShellDExecutor.class, executor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(getTestRecord(), getTestRecord(), getTestRecord()));
    runner.runDestroy();

    assertHandoverFile("a\na\na\n");
  }

  @Test
  public void testEnvironmentalVariables() throws Exception {
    config.script = "echo ${company} > " + handoverFile.getPath();
    config.environmentVariables = Collections.singletonMap("company", "${record:value('/company')}");

    ShellExecutor executor = new ShellExecutor(config);

    ExecutorRunner runner = new ExecutorRunner.Builder(ShellDExecutor.class, executor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(getTestRecord()));
    runner.runDestroy();

    assertHandoverFile("StreamSets\n");
  }

  @Test
  public void testFailedScript() throws Exception {
    config.script = "exit 1";

    ShellExecutor executor = new ShellExecutor(config);

    ExecutorRunner runner = new ExecutorRunner.Builder(ShellDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(getTestRecord()));
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("SHELL_003", errors.get(0).getHeader().getErrorCode());
  }


  @Test
  public void testTimeout() throws Exception {
    config.script = "sleep 10\n" +
      "echo 'a' > " + handoverFile.getPath();
    config.timeout = "1000";

    ShellExecutor executor = new ShellExecutor(config);

    ExecutorRunner runner = new ExecutorRunner.Builder(ShellDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(getTestRecord()));
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("SHELL_002", errors.get(0).getHeader().getErrorCode());
    assertHandoverFile("");
  }

  @Test
  public void testTimeoutWithEL() throws Exception {
    config.script = "sleep 10\n" +
      "echo 'a' > " + handoverFile.getPath();
    config.timeout = "${1 * SECONDS}";

    ShellExecutor executor = new ShellExecutor(config);

    ExecutorRunner runner = new ExecutorRunner.Builder(ShellDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    runner.runWrite(ImmutableList.of(getTestRecord()));
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("SHELL_002", errors.get(0).getHeader().getErrorCode());
    assertHandoverFile("");
  }
}
