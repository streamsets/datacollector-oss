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
package com.streamsets.pipeline.stage.destination.recordtolocalfilesystem;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.recordstolocalfilesystem.ToErrorLocalFSDTarget;
import com.streamsets.pipeline.stage.destination.recordstolocalfilesystem.RecordsToLocalFileSystemTarget;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class TestRecordsToLocalFileSystemTarget {

  private Record createRecord(String str) {
    Record record = RecordCreator.create();
    record.set(Field.create(str));
    return record;
  }

  @Test
  public void testTargetLifecycleTimeRotation() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Target target = new RecordsToLocalFileSystemTarget(dir.getAbsolutePath(), "x", "${1}", 0);
    TargetRunner runner = new TargetRunner.Builder(ToErrorLocalFSDTarget.class, target).build();
    List<Record> input = new ArrayList<>();
    input.add(createRecord("a"));
    input.add(createRecord("b"));
    Assert.assertTrue(runner.runValidateConfigs().isEmpty());
    runner.runInit();
    try {
      runner.runWrite(input);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());
      Thread.sleep(1100);
      runner.runWrite(input);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());
    } finally {
      runner.runDestroy();
    }
    Assert.assertEquals(Arrays.toString(dir.list()), 2, dir.list().length);
  }

  @Test
  public void testTargetLifecycleSizeRotation() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Target target = new RecordsToLocalFileSystemTarget(dir.getAbsolutePath(), "x", "${10}", 1);
    TargetRunner runner = new TargetRunner.Builder(ToErrorLocalFSDTarget.class, target).build();
    List<Record> input = new ArrayList<>();
    for (int i = 0; i < 5000; i++) {
      input.add(createRecord(Integer.toString(i)));
    }
    Assert.assertTrue(runner.runValidateConfigs().isEmpty());
    runner.runInit();
    try {
      runner.runWrite(input);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());
    } finally {
      runner.runDestroy();
    }
    Assert.assertTrue(dir.list().length > 1);
  }

  @Test
  public void testTargetLifecycleSizeTimeRotation() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Target target = new RecordsToLocalFileSystemTarget(dir.getAbsolutePath(), "x", "${1}", 1);
    TargetRunner runner = new TargetRunner.Builder(ToErrorLocalFSDTarget.class, target).build();
    List<Record> input = new ArrayList<>();
    for (int i = 0; i < 5000; i++) {
      input.add(createRecord(Integer.toString(i)));
    }
    Assert.assertTrue(runner.runValidateConfigs().isEmpty());
    runner.runInit();
    try {
      runner.runWrite(input);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());
      Thread.sleep(1001);
      runner.runWrite(input);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());
    } finally {
      runner.runDestroy();
    }
    Assert.assertTrue(dir.list().length > 2);
  }

}
