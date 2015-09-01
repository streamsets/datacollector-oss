/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.config.ModelType;

import org.junit.Assert;
import org.junit.Test;

public class TestModelTypeBean {

  @Test
  public void testModelTypeBean() {
    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.LIST_BEAN) ==
      ModelTypeJson.LIST_BEAN);
    Assert.assertTrue(BeanHelper.unwrapModelType(ModelTypeJson.LIST_BEAN) ==
      ModelType.LIST_BEAN);

    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.FIELD_SELECTOR) ==
      ModelTypeJson.FIELD_SELECTOR);
    Assert.assertTrue(
      BeanHelper.unwrapModelType(ModelTypeJson.FIELD_SELECTOR) ==
      ModelType.FIELD_SELECTOR);

    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.FIELD_SELECTOR_MULTI_VALUE) ==
      ModelTypeJson.FIELD_SELECTOR_MULTI_VALUE);
    Assert.assertTrue(
      BeanHelper.unwrapModelType(ModelTypeJson.FIELD_SELECTOR_MULTI_VALUE) ==
        ModelType.FIELD_SELECTOR_MULTI_VALUE);

    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.FIELD_VALUE_CHOOSER) ==
      ModelTypeJson.FIELD_VALUE_CHOOSER);
    Assert.assertTrue(BeanHelper.unwrapModelType(ModelTypeJson.FIELD_VALUE_CHOOSER) ==
      ModelType.FIELD_VALUE_CHOOSER);

    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.PREDICATE) ==
      ModelTypeJson.PREDICATE);
    Assert.assertTrue(BeanHelper.unwrapModelType(ModelTypeJson.PREDICATE) ==
      ModelType.PREDICATE);

    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.VALUE_CHOOSER) ==
      ModelTypeJson.VALUE_CHOOSER);
    Assert.assertTrue(BeanHelper.unwrapModelType(ModelTypeJson.VALUE_CHOOSER) ==
      ModelType.VALUE_CHOOSER);
  }
}
