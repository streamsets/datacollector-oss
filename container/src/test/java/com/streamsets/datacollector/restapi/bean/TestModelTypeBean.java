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
