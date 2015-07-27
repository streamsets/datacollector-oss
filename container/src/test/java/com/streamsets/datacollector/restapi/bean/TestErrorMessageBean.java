/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
