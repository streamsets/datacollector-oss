/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
    Target target = new RecordsToLocalFileSystemTarget(dir.getAbsolutePath(), "x", "${900}", 0);
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
      Thread.sleep(1001);
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
