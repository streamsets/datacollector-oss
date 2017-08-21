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
package com.streamsets.datacollector.restapi.bean;

import org.junit.Assert;
import org.junit.Test;

import com.streamsets.datacollector.restapi.bean.ErrorMessageJson;

public class TestErrorMessageBean {

  @Test(expected = NullPointerException.class)
  public void testErrorMessageBeanNull() {
    ErrorMessageJson errorMessageJson = new ErrorMessageJson(null);
  }

  @Test
  public void testErrorMessageBean() {
    com.streamsets.pipeline.api.impl.ErrorMessage errorMessage =
      new com.streamsets.pipeline.api.impl.ErrorMessage("errorCode", "Hello World", System.currentTimeMillis());

    ErrorMessageJson errorMessageJsonBean = new ErrorMessageJson(errorMessage);

    Assert.assertEquals(errorMessage.getErrorCode(), errorMessageJsonBean.getErrorCode());
    Assert.assertEquals(errorMessage.getLocalized(), errorMessageJsonBean.getLocalized());
    Assert.assertEquals(errorMessage.getNonLocalized(), errorMessageJsonBean.getNonLocalized());
    Assert.assertEquals(errorMessage.getTimestamp(), errorMessageJsonBean.getTimestamp());

    //check underlying
    Assert.assertEquals(errorMessage.getErrorCode(), errorMessageJsonBean.getErrorMessage().getErrorCode());
    Assert.assertEquals(errorMessage.getLocalized(), errorMessageJsonBean.getErrorMessage().getLocalized());
    Assert.assertEquals(errorMessage.getNonLocalized(), errorMessageJsonBean.getErrorMessage().getNonLocalized());
    Assert.assertEquals(errorMessage.getTimestamp(), errorMessageJsonBean.getErrorMessage().getTimestamp());
  }

}
