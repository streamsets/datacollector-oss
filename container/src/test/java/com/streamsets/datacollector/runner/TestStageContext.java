/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.icegreen.greenmail.util.ServerSetup;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.email.EmailException;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.record.EventRecordImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.ErrorSink;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

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
    StageContext context = new StageContext(
        "stage",
        StageType.SOURCE,
        false,
        OnRecordError.TO_ERROR,
        Collections.EMPTY_LIST,
        Collections.EMPTY_MAP,
        Collections.<String, Object> emptyMap(),
        ExecutionMode.STANDALONE,
        null,
        new EmailSender(new Configuration())
    );

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
    StageContext context = new StageContext(
        "stage",
        StageType.SOURCE,
        false,
        OnRecordError.TO_ERROR,
        Collections.EMPTY_LIST,
        Collections.EMPTY_MAP,
        Collections.<String, Object> emptyMap(),
        ExecutionMode.STANDALONE,
        null,
        new EmailSender(new Configuration())
    );

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
    StageContext context = new StageContext(
        "stage",
        StageType.SOURCE,
        false,
        OnRecordError.TO_ERROR,
        Collections.EMPTY_LIST,
        Collections.EMPTY_MAP,
        Collections.<String, Object> emptyMap(),
        ExecutionMode.STANDALONE,
        null,
        new EmailSender(new Configuration())
    );

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
    StageContext context = new StageContext(
        "stage",
        type,
        false,
        OnRecordError.TO_ERROR,
        Collections.EMPTY_LIST,
        Collections.EMPTY_MAP,
        Collections.<String, Object> emptyMap(),
        ExecutionMode.STANDALONE,
        null,
        new EmailSender(new Configuration())
    );

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
      false,
      OnRecordError.TO_ERROR,
      Collections.EMPTY_LIST,
      Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap(),
      ExecutionMode.STANDALONE,
      null,
      sender
    );

    try {
      context.notify(ImmutableList.of("foo", "bar"), "SUBJECT", "BODY");
      Assert.fail("Expected StageException");
    } catch (StageException e) {

    }

  }

  @Test
  public void testNotify() throws StageException, EmailException {

    EmailSender sender = Mockito.mock(EmailSender.class);

    StageContext context = new StageContext(
      "stage",
      StageType.SOURCE,
      false,
      OnRecordError.TO_ERROR,
      Collections.EMPTY_LIST,
      Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap(),
      ExecutionMode.STANDALONE,
      null,
      sender
    );

    context.notify(ImmutableList.of("foo", "bar"), "SUBJECT", "BODY");
    Mockito.verify(sender, Mockito.times(1)).send(
        Mockito.eq(ImmutableList.of("foo", "bar")),
        Mockito.eq("SUBJECT"),
        Mockito.eq("BODY"));

  }

  @Test
  public void testEventRecordCreation() throws StageException, EmailException {
    StageContext context = new StageContext(
      "stage",
      StageType.SOURCE,
      false,
      OnRecordError.TO_ERROR,
      Collections.EMPTY_LIST,
      Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap(),
      ExecutionMode.STANDALONE,
      null,
      new EmailSender(new Configuration())
    );

    EventRecord event = context.createEventRecord("custom_type", 2);
    Assert.assertNotNull(event);
    Assert.assertEquals("custom_type", event.getHeader().getAttribute(EventRecord.TYPE));
    Assert.assertEquals("2", event.getHeader().getAttribute(EventRecord.VERSION));
    Assert.assertNotNull(event.getHeader().getAttribute(EventRecord.CREATION_TIMESTAMP));
  }

  @Test
  public void testToEvent() throws Exception {
    StageContext context = new StageContext(
        "stage",
        StageType.SOURCE,
        false,
        OnRecordError.TO_ERROR,
        Collections.EMPTY_LIST,
        Collections.EMPTY_MAP,
        Collections.<String, Object> emptyMap(),
        ExecutionMode.STANDALONE,
        null,
        new EmailSender(new Configuration())
    );

    EventSink sink = new EventSink();
    context.setEventSink(sink);

    EventRecord event = new EventRecordImpl("custom-type", 1, "local-stage", "super-secret-id", null, null);
    event.set(Field.create(ImmutableMap.of("key", Field.create("value"))));
    context.toEvent(event);
    Assert.assertEquals(1, sink.getEventRecords().size());
    Record retrieved = sink.getEventRecords().get(0);

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

}
