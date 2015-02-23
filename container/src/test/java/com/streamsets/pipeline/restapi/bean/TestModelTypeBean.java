/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.streamsets.pipeline.config.ModelType;
import org.junit.Assert;
import org.junit.Test;

public class TestModelTypeBean {

  @Test
  public void testModelTypeBean() {
    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.COMPLEX_FIELD) ==
      ModelTypeJson.COMPLEX_FIELD);
    Assert.assertTrue(BeanHelper.unwrapModelType(ModelTypeJson.COMPLEX_FIELD) ==
      ModelType.COMPLEX_FIELD);

    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.FIELD_SELECTOR_SINGLE_VALUED) ==
      ModelTypeJson.FIELD_SELECTOR_SINGLE_VALUED);
    Assert.assertTrue(
      BeanHelper.unwrapModelType(ModelTypeJson.FIELD_SELECTOR_SINGLE_VALUED) ==
      ModelType.FIELD_SELECTOR_SINGLE_VALUED);

    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.FIELD_SELECTOR_MULTI_VALUED) ==
      ModelTypeJson.FIELD_SELECTOR_MULTI_VALUED);
    Assert.assertTrue(
      BeanHelper.unwrapModelType(ModelTypeJson.FIELD_SELECTOR_MULTI_VALUED) ==
        ModelType.FIELD_SELECTOR_MULTI_VALUED);

    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.FIELD_VALUE_CHOOSER) ==
      ModelTypeJson.FIELD_VALUE_CHOOSER);
    Assert.assertTrue(BeanHelper.unwrapModelType(ModelTypeJson.FIELD_VALUE_CHOOSER) ==
      ModelType.FIELD_VALUE_CHOOSER);

    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.LANE_PREDICATE_MAPPING) ==
      ModelTypeJson.LANE_PREDICATE_MAPPING);
    Assert.assertTrue(BeanHelper.unwrapModelType(ModelTypeJson.LANE_PREDICATE_MAPPING) ==
      ModelType.LANE_PREDICATE_MAPPING);

    Assert.assertTrue(BeanHelper.wrapModelType(ModelType.VALUE_CHOOSER) ==
      ModelTypeJson.VALUE_CHOOSER);
    Assert.assertTrue(BeanHelper.unwrapModelType(ModelTypeJson.VALUE_CHOOSER) ==
      ModelType.VALUE_CHOOSER);
  }
}
