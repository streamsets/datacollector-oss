/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.record.RecordImpl;
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
      Collections.<String, Object> emptyMap());

    ErrorSink errorSink = new ErrorSink();
    context.setErrorSink(errorSink);

    Record record = new RecordImpl("s", "id", null, null);
    Exception ex = new Exception("BAR");
    context.toError(record, ex);
    Assert.assertEquals(1, errorSink.getTotalErrorRecords());
    Record eRecord = errorSink.getErrorRecords().get("stage").get(0);
    Assert.assertEquals(record.getHeader().getSourceId(), eRecord.getHeader().getSourceId());
    Assert.assertEquals("CONTAINER_0001", eRecord.getHeader().getErrorCode());
    Assert.assertEquals("CONTAINER_0001 - BAR", eRecord.getHeader().getErrorMessage());
  }

  @Test
  public void testToErrorString() throws Exception {
    StageContext context = new StageContext("stage", StageType.SOURCE, false, OnRecordError.TO_ERROR,
                                            Collections.EMPTY_LIST, Collections.EMPTY_MAP,
      Collections.<String, Object> emptyMap());

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
      Collections.<String, Object> emptyMap());

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
      Collections.<String, Object> emptyMap());

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
