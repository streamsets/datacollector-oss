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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.email.EmailException;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineageEventImpl;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.record.EventRecordImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.fail;

@SuppressWarnings("unchecked")
public class TestStageContext {

  public enum TestError implements ErrorCode {
    TEST;

    @Override
    public String getCode() {
      return TEST.name();
    }

    @Override
    public String getMessage() {
      return "FOO:{}";
    }

  }

  @Test
  public void testToErrorNonStageException() throws Exception {
    StageContext context = createStageContextForSDK();

    ErrorSink errorSink = new ErrorSink();
    context.setErrorSink(errorSink);

    Record record = new RecordImpl("s", "id", null, null);
    Exception ex = new Exception("BAR");
    context.toError(record, ex);
    Assert.assertEquals(1, errorSink.getTotalErrorRecords());
    Record eRecord = errorSink.getErrorRecords().get("stage").get(0);
    Assert.assertEquals(record.getHeader().getSourceId(), eRecord.getHeader().getSourceId());
    Assert.assertEquals("CONTAINER_0001", eRecord.getHeader().getErrorCode());
    Assert.assertTrue(eRecord.getHeader().getErrorMessage().contains("CONTAINER_0001"));
    Assert.assertTrue(eRecord.getHeader().getErrorMessage().contains("BAR"));
  }

  @Test
  public void testToErrorString() throws Exception {
    StageContext context = createStageContextForSDK();

    ErrorSink errorSink = new ErrorSink();
    context.setErrorSink(errorSink);

    Record record = new RecordImpl("s", "id", null, null);
    context.toError(record, "FOO");
    Assert.assertEquals(1, errorSink.getTotalErrorRecords());
    Record eRecord = errorSink.getErrorRecords().get("stage").get(0);
    Assert.assertEquals(record.getHeader().getSourceId(), eRecord.getHeader().getSourceId());
    Assert.assertEquals("CONTAINER_0002", eRecord.getHeader().getErrorCode());
    Assert.assertEquals("CONTAINER_0002 - FOO", eRecord.getHeader().getErrorMessage());
  }

  @Test
  public void testToErrorMessage() throws Exception {
    StageContext context = createStageContextForSDK();

    ErrorSink errorSink = new ErrorSink();
    context.setErrorSink(errorSink);

    Record record = new RecordImpl("s", "id", null, null);
    context.toError(record, TestError.TEST, "BAR");
    Assert.assertEquals(1, errorSink.getTotalErrorRecords());
    Record eRecord = errorSink.getErrorRecords().get("stage").get(0);
    Assert.assertEquals(record.getHeader().getSourceId(), eRecord.getHeader().getSourceId());
    Assert.assertEquals("TEST", eRecord.getHeader().getErrorCode());
    Assert.assertEquals("TEST - FOO:BAR", eRecord.getHeader().getErrorMessage());
  }

  private void testToErrorStageException(StageType type) throws Exception {
    StageContext context = createStageContextForSDK();

    ErrorSink errorSink = new ErrorSink();
    context.setErrorSink(errorSink);

    Record record = new RecordImpl("s", "id", null, null);
    Exception ex = new StageException(TestError.TEST, "BAR");
    context.toError(record, ex);
    Assert.assertEquals(1, errorSink.getTotalErrorRecords());
    Record eRecord = errorSink.getErrorRecords().get("stage").get(0);
    Assert.assertEquals(record.getHeader().getSourceId(), eRecord.getHeader().getSourceId());
    Assert.assertEquals("TEST", eRecord.getHeader().getErrorCode());
    Assert.assertEquals("TEST - FOO:BAR", eRecord.getHeader().getErrorMessage());
    Record sourceRecordForERecord = ((RecordImpl)eRecord).getHeader().getSourceRecord();
    Assert.assertNotNull(sourceRecordForERecord);
    Assert.assertEquals("Source Record should be same as Error Record", eRecord, sourceRecordForERecord);
  }

  @Test
  public void testToErrorStageExceptionSource() throws Exception {
    testToErrorStageException(StageType.SOURCE);
  }

  @Test
  public void testToErrorStageExceptionProcessor() throws Exception {
    testToErrorStageException(StageType.PROCESSOR);

  }

  @Test
  public void testToErrorStageExceptionTarget() throws Exception {
    testToErrorStageException(StageType.TARGET);
  }

  @Test
  public void testNotifyException() throws EmailException {

    EmailSender sender = Mockito.mock(EmailSender.class);
    Mockito.doThrow(StageException.class)
        .when(sender)
        .send(Mockito.anyList(), Mockito.anyString(), Mockito.anyString());

    StageContext context = new StageContext(
      "stage",
      StageType.SOURCE,
      -1,
      false,
      OnRecordError.TO_ERROR,
      Collections.EMPTY_LIST,
      Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap(),
      ExecutionMode.STANDALONE,
      DeliveryGuarantee.AT_LEAST_ONCE,
      null,
      sender,
      new Configuration(),
      new LineagePublisherDelegator.NoopDelegator(),
      Mockito.mock(RuntimeInfo.class),
      Collections.emptyMap()
    );

    try {
      context.notify(ImmutableList.of("foo", "bar"), "SUBJECT", "BODY");
      fail("Expected StageException");
    } catch (StageException e) {

    }

  }

  @Test
  public void testNotify() throws StageException, EmailException {

    EmailSender sender = Mockito.mock(EmailSender.class);

    StageContext context = new StageContext(
      "stage",
      StageType.SOURCE,
      -1,
      false,
      OnRecordError.TO_ERROR,
      Collections.EMPTY_LIST,
      Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap(),
      ExecutionMode.STANDALONE,
      DeliveryGuarantee.AT_LEAST_ONCE,
      null,
      sender,
      new Configuration(),
      new LineagePublisherDelegator.NoopDelegator(),
      Mockito.mock(RuntimeInfo.class),
      Collections.emptyMap()
    );

    context.notify(ImmutableList.of("foo", "bar"), "SUBJECT", "BODY");
    Mockito.verify(sender, Mockito.times(1)).send(
        Mockito.eq(ImmutableList.of("foo", "bar")),
        Mockito.eq("SUBJECT"),
        Mockito.eq("BODY"));

  }

  @Test
  public void testEventRecordCreation() throws StageException, EmailException {
    StageContext context = createStageContextForSDK();

    EventRecord event = context.createEventRecord("custom_type", 2, "eventSourceId");
    Assert.assertNotNull(event);
    Assert.assertEquals("custom_type", event.getHeader().getAttribute(EventRecord.TYPE));
    Assert.assertEquals("2", event.getHeader().getAttribute(EventRecord.VERSION));
    Assert.assertNotNull(event.getHeader().getAttribute(EventRecord.CREATION_TIMESTAMP));
  }

  @Test
  public void testMissingSpecificAttributes() throws Exception {
    StageContext context = createStageContextForSDK();

    for (int ok = 0; ok < 2; ok++) {
      for (LineageEventType type : LineageEventType.values()) {
        for (LineageSpecificAttribute attr : type.getSpecificAttributes()) {
          LineageEvent event = new LineageEventImpl(
              type,
              "pipelineTitle",
              "sdk-user",
              System.currentTimeMillis(),
              "pipelineId",
              "sdc-id",
              "http://streamsets.com",
              "stageName"
          );
          for (LineageSpecificAttribute other : type.getSpecificAttributes()) {
            if (ok % 2 == 1) {
              if (!other.equals(attr)) {
                event.setSpecificAttribute(other, "something");
              } else {
                event.setSpecificAttribute(other, "something");
              }
            } else {
              event.setSpecificAttribute(other, "something_else");
            }
            try {
              context.publishLineageEvent(event);
            } catch (IllegalArgumentException ex) {
              Assert.assertTrue(ex.getMessage().contains("Missing or Empty SpecificAttributes"));
            }
          }
        }
      }
    }
  }

  @Test
  public void testToEvent() throws Exception {
    StageContext context = createStageContextForSDK();

    EventSink sink = new EventSink();
    context.setEventSink(sink);

    EventRecord event = new EventRecordImpl("custom-type", 1, "local-stage", "super-secret-id", null, null);
    event.set(Field.create(ImmutableMap.of("key", Field.create("value"))));
    context.toEvent(event);
    Assert.assertEquals(1, sink.getStageEvents("stage").size());
    Record retrieved = sink.getStageEvents("stage").get(0);

    // Header is properly propagated
    Assert.assertEquals("custom-type", retrieved.getHeader().getAttribute(EventRecord.TYPE));
    Assert.assertEquals("1", retrieved.getHeader().getAttribute(EventRecord.VERSION));

    // Data
    Field rootField = retrieved.get();
    Assert.assertEquals(Field.Type.MAP, rootField.getType());
    Map<String, Field> map = rootField.getValueAsMap();
    Assert.assertNotNull(map);
    Assert.assertTrue(map.containsKey("key"));
    Assert.assertEquals("value", map.get("key").getValueAsString());
  }

  @SuppressWarnings("unchecked")
  private <T> T createMetrics(StageContext context, String metricName, Class<T> metricClass, final Object value) {
    if (metricClass.equals(Meter.class)) {
      Meter m = context.createMeter(metricName);
      m.mark((long)value);
      return (T)m;
    } else if (metricClass.equals(Counter.class)) {
      Counter c = context.createCounter(metricName);
      c.inc((long)value);
      return (T)c;
    } else if (metricClass.equals(Timer.class)) {
      Timer t = context.createTimer(metricName);
      t.update((long)value, TimeUnit.NANOSECONDS);
      return (T)t;
    } else if (metricClass.equals(Histogram.class)) {
      Histogram h = context.createHistogram(metricName);
      h.update((long)value);
      return (T)h;
    } else {
      Gauge<Map<String, Object>> g = context.createGauge(metricName);
      g.getValue().putAll(((Map)value));
      return (T)g;
    }
  }

  @Test
  public void testCreateAndGetMeter() throws Exception {
    StageContext context = createStageContextForSDK();
    String metricName = "testCreateAndGetMetrics";
    //Check non existing
    Assert.assertNull(context.getMeter(metricName));
    //Check existing
    createMetrics(context, metricName, Meter.class, 1000L);
    Meter m = context.getMeter(metricName);
    Assert.assertNotNull(m);
    Assert.assertEquals(1000, m.getCount());
  }

  @Test
  public void testCreateAndGetTimer() throws Exception {
    StageContext context = createStageContextForSDK();
    String metricName = "testCreateAndGetMetrics";
    //Check non existing
    Assert.assertNull(context.getTimer(metricName));
    //Check existing
    createMetrics(context, metricName, Timer.class, 1000L);
    Timer t = context.getTimer(metricName);
    Assert.assertNotNull(t);
    Assert.assertEquals(1000, t.getSnapshot().getValues()[0]);
  }

  @Test
  public void testCreateAndGetCounter() throws Exception {
    StageContext context = createStageContextForSDK();
    String metricName = "testCreateAndGetMetrics";
    //Check non existing
    Assert.assertNull(context.getCounter(metricName));
    //Check existing
    createMetrics(context, metricName, Counter.class, 1000L);
    Counter c = context.getCounter(metricName);
    Assert.assertNotNull(c);
    Assert.assertEquals(1000, c.getCount());
  }

  @Test
  public void testCreateAndGetHistogram() throws Exception {
    StageContext context = createStageContextForSDK();
    String metricName = "testCreateAndGetHistgoram";
    //Check non existing
    Assert.assertNull(context.getHistogram(metricName));
    //Check existing
    createMetrics(context, metricName, Histogram.class, 1000L);
    Histogram h = context.getHistogram(metricName);
    Assert.assertNotNull(h);
    Assert.assertEquals(1, h.getCount());
  }

  @Test
  public void testCreateAndGetGauge() throws Exception {
    StageContext context = createStageContextForSDK();
    String metricName = "testCreateAndGetMetrics";
    //Check non existing
    Assert.assertNull(context.getGauge(metricName));
    //Check existing
    Map<String, Integer> gaugeMap = new HashMap<>();
    gaugeMap.put("1", 1);
    gaugeMap.put("2", 2);
    gaugeMap.put("3", 3);
    createMetrics(context, metricName, Gauge.class, gaugeMap);
    Gauge<Map<String, Object>> g = context.getGauge(metricName);
    Assert.assertNotNull(g);
    Assert.assertEquals(gaugeMap, g.getValue());
    g.getValue().remove("3");
    g =  context.getGauge(metricName);
    Assert.assertTrue(g.getValue().containsKey("1"));
    Assert.assertTrue(g.getValue().containsKey("2"));
    Assert.assertFalse(g.getValue().containsKey("3"));
  }

  @Test
  public void testGetConf() throws StageException, EmailException {
    StageContext context = createStageContextForSDK();

    Assert.assertNull(context.getConfig("jarcec"));
    Assert.assertEquals("is awesome", context.getConfig("girish"));
  }

  @Test
  public void testPushSourceContextDelegate() throws Exception {
    StageContext context = createStageContextForSDK();

    final AtomicBoolean startBatchCalled = new AtomicBoolean();
    final AtomicBoolean processBatchCalled = new AtomicBoolean();
    final AtomicBoolean commitOffsetCalled = new AtomicBoolean();

    context.setPushSourceContextDelegate(new PushSourceContextDelegate() {
      @Override
      public BatchContext startBatch() {
        startBatchCalled.set(true);
        return null;
      }

      @Override
      public boolean processBatch(BatchContext batchContext, String entity, String offset) {
        processBatchCalled.set(true);
        return false;
      }

      @Override
      public void commitOffset(String entity, String offset) {
        commitOffsetCalled.set(true);
      }
    });

    context.startBatch();
    Assert.assertTrue(startBatchCalled.get());

    context.processBatch(null);
    Assert.assertTrue(processBatchCalled.get());

    context.commitOffset("entity", "offset");
    Assert.assertTrue(commitOffsetCalled.get());
  }

  @Test
  public void testIds() {
    StageContext context = createStageContextForSDK();
    Assert.assertEquals("stage", context.getStageInfo().getInstanceName());
    Assert.assertEquals("mySDC", context.getSdcId());
    Assert.assertEquals("myPipeline", context.getPipelineId());
  }

  private StageContext createStageContextForSDK() {
    Configuration configuration = new Configuration();
    configuration.set("stage.conf_girish", "is awesome");
    configuration.set("stage.conf_arvind", "is Arvind");

    return new StageContext(
      "stage",
      StageType.SOURCE,
      -1,
      false,
      OnRecordError.TO_ERROR,
      Collections.EMPTY_LIST,
      Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap(),
      ExecutionMode.STANDALONE,
      DeliveryGuarantee.AT_LEAST_ONCE,
      null,
      new EmailSender(new Configuration()),
      configuration,
      new LineagePublisherDelegator.NoopDelegator(),
      Mockito.mock(RuntimeInfo.class),
      Collections.emptyMap()
    );
  }
}
