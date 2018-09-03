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
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

public class TestFilterRecordBatch {

  @SuppressWarnings("unchecked")
  private StageContext createContext(OnRecordError onRecordError) {
    return new StageContext(
      "i",
      StageType.PROCESSOR,
      -1,
      true,
      onRecordError,
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

  public StageContext runBaseTest(OnRecordError onRecordError) {
    StageContext context = createContext(onRecordError);
    context.getErrorSink().registerInterceptorsForStage("i", Collections.emptyList());

    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(context, Arrays.asList("${record:value('/') == 'Hello'}"));

    Record record = new RecordImpl("", "", null, null);
    record.set(Field.create("Ahoj"));

    Batch batch = new BatchImpl("instance", "source", "offset", ImmutableList.of(record));

    FilterRecordBatch filterBatch = new FilterRecordBatch(batch, new FilterRecordBatch.Predicate[]{predicate}, context);

    Iterator<Record> records = filterBatch.getRecords();
    Assert.assertFalse(records.hasNext());

    return context;
  }

  @Test
  public void testOnRecordErrorIgnore() throws Exception {
    StageContext context = runBaseTest(OnRecordError.DISCARD);
    Assert.assertEquals(0, context.getErrorSink().getErrorRecords("i").size());
  }

  @Test(expected = RuntimeException.class)
  public void testOnRecordErrorStopPipeline() {
    runBaseTest(OnRecordError.STOP_PIPELINE);
  }

  @Test
  public void testOnRecordErrorToError() throws Exception {
    StageContext context = runBaseTest(OnRecordError.TO_ERROR);
    Assert.assertEquals(1, context.getErrorSink().getErrorRecords("i").size());
  }

  // This case will happen if stage have @HideConfigs(onErrorRecord = true)
  @Test
  public void testOnRecordErrorNull() throws Exception {
    StageContext context = runBaseTest(OnRecordError.TO_ERROR);
    Assert.assertEquals(1, context.getErrorSink().getErrorRecords("i").size());
  }

  @Test
  public void testFilterRecordBatchIterator() {
    StageContext context = createContext(OnRecordError.TO_ERROR);
    FilterRecordBatch.Predicate predicate = new PreconditionsPredicate(context, Arrays.asList("${record:exists('/pass')}"));

    // 3 sample records: [invalid, pass, invalid]
    Record record1 = new RecordImpl("", "", null, null);
    record1.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "invalid", Field.create(Field.Type.STRING, "Precondition should fail")
    )));
    Record record2 = new RecordImpl("", "", null, null);
    record2.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "pass", Field.create(Field.Type.STRING, "passed record")
    )));
    Record record3 = new RecordImpl("", "", null, null);
    record3.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "invalid", Field.create(Field.Type.STRING, "Precondition should fail")
    )));

    Batch batch = new BatchImpl("i", "s", "o", ImmutableList.of(record1, record2, record3));
    FilterRecordBatch filterBatch = new FilterRecordBatch(batch, new FilterRecordBatch.Predicate[]{predicate}, context);

    Iterator<Record> ite = filterBatch.getRecords();
    Assert.assertEquals(true, ite.hasNext());
    // Above hasNext() should send record1 to error
    Assert.assertEquals(2, context.getErrorSink().size());

    // Iterator should be still pointing to record2
    Assert.assertEquals(true, ite.hasNext());
    Record r = ite.next();
    Assert.assertEquals("passed record", r.get("/pass").getValueAsString());

    // Iterator should be pointing to record3 and send it to error
    Assert.assertFalse(ite.hasNext());
    Assert.assertEquals(2, context.getErrorSink().size());

    // Get a new iterator to iterate again. Size of error records should stay 2
    Iterator<Record> ite2 = filterBatch.getRecords();
    Assert.assertEquals(true, ite2.hasNext());
    Assert.assertEquals(2, context.getErrorSink().size());

    // Iterator should be still pointing to record2
    Assert.assertEquals("passed record", ite2.next().get("/pass").getValueAsString());
    Assert.assertEquals(2, context.getErrorSink().size());
  }
}
