/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.api;

import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.container.LocaleInContext;
import com.streamsets.pipeline.container.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Locale;

public class TestStageException {

  @After
  public void cleanUp() {
    LocaleInContext.set(null);
  }

  public enum TErrorId implements ErrorId {
    ID0("hi"),
    ID1("hello '{}'");

    private String template;

    private TErrorId(String template) {
      this.template = template;
    }

    @Override
    public String getMessageTemplate() {
      return template;
    }
  }

  @Test(expected = NullPointerException.class)
  @SuppressWarnings("ThrowableInstanceNeverThrown")
  public void testConstructorFail1() {
    new StageException(null);
  }

  @Test(expected = NullPointerException.class)
  @SuppressWarnings("ThrowableInstanceNeverThrown")
  public void testConstructorFail2() {
    new StageException(TErrorId.ID0, (Object[])null);
  }

  @Test
  public void testConstructorOK() {
    StageException ex = new StageException(TErrorId.ID0);
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull(TErrorId.ID0.getMessageTemplate(), ex.getMessage());
    Assert.assertEquals(TErrorId.ID0, ex.getErrorId());
    ex = new StageException(TErrorId.ID1);
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull("hello '{}'", ex.getMessage());
    Assert.assertEquals(TErrorId.ID1, ex.getErrorId());
    Exception cause = new Exception();
    ex = new StageException(TErrorId.ID0, cause);
    Assert.assertEquals(cause, ex.getCause());
  }

  @Test
  public void testMethodDelegation() {
    StageException ex = new StageException(TErrorId.ID0, new Exception());
    ex.getCause();
    ex.getErrorId();
    ex.getLocalizedMessage();
    ex.getMessage();
    ex.getMessage();
    ex.getStackTrace();
    ex.printStackTrace();
    ex.printStackTrace(Mockito.mock(PrintStream.class));
    ex.printStackTrace(Mockito.mock(PrintWriter.class));
    ex.setStackTrace(new StackTraceElement[0]);
    Assert.assertNotNull(ex.toString());
  }


  @Test
  public void testMessageLocalizationWithStageContext() {
    try {
      Stage.Info info = Mockito.mock(Stage.Info.class);
      Mockito.when(info.getName()).thenReturn("stage");
      Mockito.when(info.getVersion()).thenReturn("1.0.0");
      Utils.setStageExceptionContext(info, getClass().getClassLoader());

      StageException ex = new StageException(TErrorId.ID0);
      LocaleInContext.set(null);
      Assert.assertNotNull("HI", ex.getLocalizedMessage());

      ex = new StageException(TErrorId.ID1, "foo");
      LocaleInContext.set(Locale.getDefault());
      Assert.assertNotNull("HELLO 'foo'", ex.getLocalizedMessage());

      // testing pipeline-api bundle
      ex = new StageException(SingleLaneProcessor.ERROR.OUTPUT_LANE_ERROR, 2);
      LocaleInContext.set(Locale.getDefault());
      Assert.assertFalse(ex.getLocalizedMessage().endsWith(" "));

    } finally {
      Utils.resetStageExceptionContext();
    }
  }

  @Test
  public void testMissingResourceBundle() {
    try {
      Stage.Info info = Mockito.mock(Stage.Info.class);
      Mockito.when(info.getName()).thenReturn("missing");
      Mockito.when(info.getVersion()).thenReturn("1.0.0");
      Utils.setStageExceptionContext(info, getClass().getClassLoader());

      StageException ex = new StageException(TErrorId.ID0);
      LocaleInContext.set(null);
      Assert.assertNotNull("hi", ex.getLocalizedMessage());

    } finally {
      Utils.resetStageExceptionContext();
    }
  }

}
