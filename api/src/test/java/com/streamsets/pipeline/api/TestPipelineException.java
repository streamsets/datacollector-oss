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

import com.streamsets.pipeline.api.PipelineException.ID;
import org.junit.Assert;
import org.junit.Test;

import java.util.ResourceBundle;

public class TestPipelineException {

  public enum TID implements ID {
    ID0("hi"),
    ID1("hello '{}'");

    private String template;

    private TID(String template) {
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
    new PipelineException(null);
  }

  @Test(expected = NullPointerException.class)
  @SuppressWarnings("ThrowableInstanceNeverThrown")
  public void testConstructorFail2() {
    new PipelineException(TID.ID0, (Object[])null);
  }

  @Test
  public void testConstructorOK() {
    PipelineException ex = new PipelineException(TID.ID0);
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull(TID.ID0.getMessageTemplate(), ex.getMessage());
    Assert.assertEquals(TID.ID0, ex.getID());
    ex = new PipelineException(TID.ID1);
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull("hello '{}'", ex.getMessage());
    Assert.assertEquals(TID.ID1, ex.getID());
    Exception cause = new Exception();
    ex = new PipelineException(TID.ID0, cause);
    Assert.assertEquals(cause, ex.getCause());
  }

  @Test
  public void testMessage() {
    PipelineException ex = new PipelineException(TID.ID0, "x");
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull(TID.ID0.getMessageTemplate(), ex.getMessage());

    ex = new PipelineException(TID.ID1, (Object) null);
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull("hello 'null'", ex.getMessage());

    ex = new PipelineException(TID.ID1, "foo");
    Assert.assertNull(ex.getCause());
    Assert.assertNotNull("hello 'foo'", ex.getMessage());
  }

  @Test
  public void testMessageLocalization() {
    ResourceBundle rb = ResourceBundle.getBundle("test-bundle");

    PipelineException ex = new PipelineException(TID.ID0);
    Assert.assertNotNull("hi", ex.getMessage(rb));

    ex = new PipelineException(TID.ID1, "foo");
    Assert.assertNotNull("hello 'foo'", ex.getMessage(null));
    Assert.assertNotNull("HELLO 'foo'", ex.getMessage(rb));
  }

}
