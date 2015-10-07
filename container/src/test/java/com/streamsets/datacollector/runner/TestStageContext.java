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

import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.ErrorSink;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

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
    StageContext context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                            Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap(), ExecutionMode.STANDALONE, null);

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
    StageContext context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                            Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap(), ExecutionMode.STANDALONE, null);

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
    StageContext context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                            Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap(), ExecutionMode.STANDALONE, null);

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

  @Test
  public void testToErrorStageException() throws Exception {
    StageContext context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                            Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap(), ExecutionMode.STANDALONE, null);

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
  }

}
