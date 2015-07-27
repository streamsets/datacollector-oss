/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.FilterRecordBatch;
import com.streamsets.datacollector.runner.PreconditionsPredicate;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestPreconditionsPredicate {

  @SuppressWarnings("unchecked")
  private Stage.Context createContext() {
    return new StageContext("i", StageType.PROCESSOR, true, null, (List) Collections.emptyList(),
                                         (Map) Collections.emptyMap(), (Map) ImmutableMap.of("a", "A"), false, "");
  }

  @Test
  public void testNullEmptyPreconditions() {
    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(createContext(), null);
    Assert.assertTrue(predicate.evaluate(null));
    predicate = new PreconditionsPredicate(createContext(), Collections.<String>emptyList());
    Assert.assertTrue(predicate.evaluate(null));
  }

  @Test
  public void testELConstantsFunctions() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Files.write(new File(dir, "sdc.properties").toPath(), Arrays.asList(""), StandardCharsets.UTF_8);
    Files.write(new File(dir, "res.txt").toPath(), Arrays.asList("R"), StandardCharsets.UTF_8);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getConfigDir()).thenReturn(dir.getAbsolutePath());
    Mockito.when(runtimeInfo.getResourcesDir()).thenReturn(dir.getAbsolutePath());
    RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(createContext(), Arrays.asList(
      "${record:value('/') == 'Hello' && a == 'A' && str:startsWith(runtime:loadResource('res.txt', false), 'R')}"));

    Record record = new RecordImpl("", "", null, null);
    record.set(Field.create("Hello"));
    Assert.assertTrue(predicate.evaluate(record));
  }

  @Test
  public void testOnePrecondition() {
    FilterRecordBatch.Predicate predicate =
        new PreconditionsPredicate(createContext(), Arrays.asList("${record:value('/') == 'Hello'}"));

    Record record = new RecordImpl("", "", null, null);
    record.set(Field.create("Hello"));
    Assert.assertTrue(predicate.evaluate(record));
    record.set(Field.create("Bye"));
    Assert.assertFalse(predicate.evaluate(record));
    Assert.assertNotNull(predicate.getRejectedMessage());
  }

  @Test
  public void testMultiplePreconditions() {
    FilterRecordBatch.Predicate predicate =
        new PreconditionsPredicate(createContext(),
                                   Arrays.asList("${record:value('/') % 2 == 0}", "${record:value('/') % 3 == 0}"));

    Record record = new RecordImpl("", "", null, null);
    record.set(Field.create(2));
    Assert.assertFalse(predicate.evaluate(record));
    Assert.assertNotNull(predicate.getRejectedMessage());
    record.set(Field.create(3));
    Assert.assertFalse(predicate.evaluate(record));
    Assert.assertNotNull(predicate.getRejectedMessage());
    record.set(Field.create(6));
    Assert.assertTrue(predicate.evaluate(record));
  }

}
