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
package com.streamsets.datacollector.validation;

import com.streamsets.pipeline.api.ErrorCode;
import org.junit.Assert;
import org.junit.Test;

public class TestIssue {

  @Test
  public void testToStringPipeline() {
    Issue pipeline = new Issue(null, null, "a", "b", RandomError.ERR_001);
    Assert.assertNotNull(pipeline.toString());
  }

  @Test
  public void testToStringStage() {
    Issue stage = new Issue("HappinessGenerator", null, "a", "b", RandomError.ERR_001);
    Assert.assertNotNull(stage.toString());
  }

  @Test
  public void testToStringService() {
    Issue service = new Issue("HappinessGenerator", "SadnessService", "a", "b", RandomError.ERR_001);
    Assert.assertNotNull(service.toString());
  }
  private enum RandomError implements ErrorCode {
    ERR_001,
    ;

    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getMessage() {
      return getCode();
    }
  }
}
