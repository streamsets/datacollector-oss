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

import org.junit.Assert;
import org.junit.Test;

public class TestStageException {

  enum TError implements ErrorId {
    ID;

    @Override
    public String getMessage() {
      return "Hello {}";
    }
  }

  public static class NonEnumError implements ErrorId {
    public static final ErrorId ID = new NonEnumError();

    @Override
    public String getMessage() {
      return "Hello {}";
    }
  }

  @Test
  public void testException() {
    StageException ex = new StageException(TError.ID);
    Assert.assertEquals(TError.ID, ex.getId());
    Assert.assertEquals("[ID] - Hello {}", ex.getMessage());
    Assert.assertEquals("[ID] - Hello {}", ex.getLocalizedMessage());
    Assert.assertNull(ex.getCause());

    Exception cause = new Exception();
    ex = new StageException(TError.ID, cause);
    Assert.assertEquals(cause, ex.getCause());

    ex = new StageException(TError.ID, "a");
    Assert.assertEquals("[ID] - Hello a", ex.getMessage());
    Assert.assertEquals("[ID] - Hello a", ex.getLocalizedMessage());
    Assert.assertNull(ex.getCause());

    ex = new StageException(TError.ID, "a", cause);
    Assert.assertEquals("[ID] - Hello a", ex.getMessage());
    Assert.assertEquals("[ID] - Hello a", ex.getLocalizedMessage());
    Assert.assertEquals(cause, ex.getCause());

    ex = new StageException(NonEnumError.ID, "a", cause);
    Assert.assertEquals(NonEnumError.ID, ex.getId());
    Assert.assertEquals("Hello a", ex.getMessage());
    Assert.assertEquals("Hello a", ex.getLocalizedMessage());
    Assert.assertEquals(cause, ex.getCause());
  }

}
