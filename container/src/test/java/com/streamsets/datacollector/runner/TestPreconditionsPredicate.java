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
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
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
import java.util.Collections;
import java.util.UUID;

public class TestPreconditionsPredicate {

  private Stage.Context createContext() {
    return new StageContext(
        "i",
        StageType.PROCESSOR,
        -1,
        true,
        null,
        Collections.emptyList(),
        Collections.emptyMap(),
        ImmutableMap.of("a", "A"),
        ExecutionMode.STANDALONE,
        DeliveryGuarantee.AT_LEAST_ONCE,
        "",
        new EmailSender(new Configuration()),
        new Configuration(),
        new LineagePublisherDelegator.NoopDelegator(),
        Mockito.mock(RuntimeInfo.class),
        Collections.emptyMap()
    );
  }

  @Test
  public void testNullEmptyPreconditions() {
    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(createContext(), null);
    Assert.assertTrue(predicate.evaluate(null));
    predicate = new PreconditionsPredicate(createContext(), Collections.emptyList());
    Assert.assertTrue(predicate.evaluate(null));
  }

  @Test
  public void testELConstantsFunctions() throws IOException {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    Files.write(new File(dir, "sdc.properties").toPath(), Collections.singletonList(""), StandardCharsets.UTF_8);
    Files.write(new File(dir, "res.txt").toPath(), Collections.singletonList("R"), StandardCharsets.UTF_8);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getConfigDir()).thenReturn(dir.getAbsolutePath());
    Mockito.when(runtimeInfo.getResourcesDir()).thenReturn(dir.getAbsolutePath());
    RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(createContext(), Collections.singletonList(
      "${record:value('/') == 'Hello' && a == 'A' && str:startsWith(runtime:loadResource('res.txt', false), 'R')}"));

    Record record = new RecordImpl("", "", null, null);
    record.set(Field.create("Hello"));
    Assert.assertTrue(predicate.evaluate(record));
  }

  @Test
  public void testOnePrecondition() {
    FilterRecordBatch.Predicate predicate =
        new PreconditionsPredicate(createContext(), Collections.singletonList("${record:value('/') == 'Hello'}"));

    Record record = new RecordImpl("", "", null, null);
    record.set(Field.create("Hello"));
    Assert.assertTrue(predicate.evaluate(record));
    record.set(Field.create("Bye"));
    Assert.assertFalse(predicate.evaluate(record));
    Assert.assertNotNull(predicate.getRejectedMessage());
  }

  @Test
  public void testMultiplePreconditions() {
    final String MODULO_TWO = "${record:value('/') % 2 == 0}";
    final String MODULO_THREE = "${record:value('/') % 3 == 0}";

    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(
      createContext(),
      ImmutableList.of(MODULO_TWO, MODULO_THREE)
    );

    Record record = new RecordImpl("", "", null, null);
    String message;

    record.set(Field.create(2));
    Assert.assertFalse(predicate.evaluate(record));
    message = predicate.getRejectedMessage().toString();
    Assert.assertNotNull(message);
    Assert.assertFalse(message, message.contains(MODULO_TWO));
    Assert.assertTrue(message, message.contains(MODULO_THREE));

    record.set(Field.create(3));
    Assert.assertFalse(predicate.evaluate(record));
    message = predicate.getRejectedMessage().toString();
    Assert.assertNotNull(message);
    Assert.assertTrue(message, message.contains(MODULO_TWO));
    Assert.assertFalse(message, message.contains(MODULO_THREE));

    record.set(Field.create(5));
    Assert.assertFalse(predicate.evaluate(record));
    message = predicate.getRejectedMessage().toString();
    Assert.assertTrue(message, message.contains(MODULO_TWO));
    Assert.assertTrue(message, message.contains(MODULO_THREE));

    record.set(Field.create(6));
    Assert.assertTrue(predicate.evaluate(record));
  }

  @Test
  public void testEvalException() throws Exception {
    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(
        createContext(),
        Collections.singletonList("${str:truncate(\"abcd\", -4) != \"abcd\"}")
    );

    Record record = new RecordImpl("", "", null, null);
    record.set(Field.create("Hello"));
    predicate.evaluate(record);
    Assert.assertNotNull(predicate.getRejectedMessage());
  }

}
