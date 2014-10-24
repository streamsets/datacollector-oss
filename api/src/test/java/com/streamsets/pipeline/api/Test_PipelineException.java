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
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

public class Test_PipelineException {
  private static final String PIPELINE_BUNDLE_NAME = "pipeline-bundle";
  
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
    new _PipelineException(PIPELINE_BUNDLE_NAME, null);
  }

  @Test(expected = NullPointerException.class)
  @SuppressWarnings("ThrowableInstanceNeverThrown")
  public void testConstructorFail2() {
    new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID0, (Object[])null);
  }

  @Test
  public void testConstructorOK() {
    _PipelineException ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID0);
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull(TErrorId.ID0.getMessageTemplate(), ex.getMessage());
    Assert.assertEquals(TErrorId.ID0, ex.getErrorId());
    ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID1);
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull("hello '{}'", ex.getMessage());
    Assert.assertEquals(TErrorId.ID1, ex.getErrorId());
    Exception cause = new Exception();
    ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID0, cause);
    Assert.assertEquals(cause, ex.getCause());
  }

  @Test
  public void testMessage() {
    _PipelineException ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID0, "x");
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull(TErrorId.ID0.getMessageTemplate(), ex.getMessage());

    ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID1, (Object) null);
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull("hello 'null'", ex.getMessage());

    ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID1, "foo");
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull("hello 'foo'", ex.getMessage());
  }

  @Test
  public void testMessageLocalizationWithDefaultBundleAndNoContext() {
    _PipelineException ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID0);
    Assert.assertNotNull("hi", ex.getMessage(null));

    ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID1, "foo");
    Assert.assertNotNull("hello 'foo'", ex.getMessage(Locale.getDefault()));

    // testing pipeline-api bundle
    ex = new _PipelineException(PIPELINE_BUNDLE_NAME, SingleLaneProcessor.Error.INPUT_LANE_ERROR, 2);
    Assert.assertFalse(ex.getMessage(Locale.getDefault()).endsWith(" "));
  }

  @Test
  public void testMessageLocalizationWithMissingBundleAndNoContext() {
    _PipelineException ex = new _PipelineException("missing-bundle", TErrorId.ID0);
    Assert.assertNotNull("hi", ex.getMessage(null));

    ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID1, "foo");
    Assert.assertNotNull("hello 'foo'", ex.getMessage(Locale.getDefault()));

    // testing pipeline-api bundle
    ex = new _PipelineException("missing-bundle", SingleLaneProcessor.Error.INPUT_LANE_ERROR, 2);
    Assert.assertTrue(ex.getMessage(Locale.getDefault()).endsWith(" "));
  }

  @Test
  public void testMessageLocalizationWithContext() {
    try {
      _PipelineException.setContext("test", getClass().getClassLoader());

      _PipelineException ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID0);
      Assert.assertNotNull("HI", ex.getMessage(null));

      ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID1, "foo");
      Assert.assertNotNull("HELLO 'foo'", ex.getMessage(Locale.getDefault()));

      // testing pipeline-api bundle
      ex = new _PipelineException(PIPELINE_BUNDLE_NAME, SingleLaneProcessor.Error.INPUT_LANE_ERROR, 2);
      Assert.assertFalse(ex.getMessage(Locale.getDefault()).endsWith(" "));

    } finally {
      _PipelineException.resetContext();
    }
  }

  @Test
  public void testMissingResourceBundle() {
    try {
      _PipelineException.setContext("invalid", getClass().getClassLoader());

      _PipelineException ex = new _PipelineException(PIPELINE_BUNDLE_NAME, TErrorId.ID0);
      Assert.assertNotNull("hi", ex.getMessage(null));

    } finally {
      _PipelineException.resetContext();
    }
  }

}
