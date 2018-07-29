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
package com.streamsets.pipeline.lib.hbase.common;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.junit.Test;

import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HBaseUtilTest {

  @Test
  public void handleHBaseException() throws Exception {
    Target.Context context = mock(Target.Context.class);
    when(context.getOnErrorRecord()).thenReturn(OnRecordError.DISCARD);

    ErrorRecordHandler errorRecordHandler = new DefaultErrorRecordHandler(context);

    List<Record> records = Collections.emptyList();

    final String errorMessage = "Some error message here.";

    Exception e = new UndeclaredThrowableException(
        new ExecutionException(new InterruptedIOException(errorMessage))
    );

    try {
      HBaseUtil.handleHBaseException(e, records.iterator(), errorRecordHandler);
    } catch (StageException ex) {
      assertEquals(StageException.class, ex.getClass());
      assertEquals(Errors.HBASE_36, ex.getErrorCode());
      assertEquals(InterruptedIOException.class, ex.getCause().getClass());
      assertEquals(errorMessage, ex.getCause().getMessage());
    }
  }

  @Test
  public void handleHBaseExceptionWithNullPointer() throws Exception {
    Target.Context context = mock(Target.Context.class);
    when(context.getOnErrorRecord()).thenReturn(OnRecordError.DISCARD);

    ErrorRecordHandler errorRecordHandler = new DefaultErrorRecordHandler(context);

    List<Record> records = new ArrayList<>();
    Record record = RecordCreator.create();
    record.set(Field.create("abc"));
    records.add(record);

    Exception e = new UndeclaredThrowableException(new ExecutionException(new NullPointerException()));

    try {
      HBaseUtil.handleHBaseException(e, records.iterator(), errorRecordHandler);
    } catch (StageException ex) {
      assertEquals(StageException.class, ex.getClass());
      assertEquals(Errors.HBASE_37, ex.getErrorCode());
      assertEquals(NullPointerException.class, ex.getCause().getClass());
    }
  }

}
