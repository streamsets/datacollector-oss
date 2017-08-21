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

import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.MetricTypeJson;

import org.junit.Assert;
import org.junit.Test;

public class TestMetricTypeBean {

  @Test
  public void testMetricTypeBean() {
    Assert.assertTrue(BeanHelper.wrapMetricType(MetricType.METER) ==
      MetricTypeJson.METER);
    Assert.assertTrue(BeanHelper.unwrapMetricType(MetricTypeJson.METER) ==
      MetricType.METER);

    Assert.assertTrue(BeanHelper.wrapMetricType(MetricType.COUNTER) ==
      MetricTypeJson.COUNTER);
    Assert.assertTrue(BeanHelper.unwrapMetricType(MetricTypeJson.COUNTER) ==
      MetricType.COUNTER);

    Assert.assertTrue(BeanHelper.wrapMetricType(MetricType.HISTOGRAM) ==
      MetricTypeJson.HISTOGRAM);
    Assert.assertTrue(BeanHelper.unwrapMetricType(MetricTypeJson.HISTOGRAM) ==
      MetricType.HISTOGRAM);

    Assert.assertTrue(BeanHelper.wrapMetricType(MetricType.TIMER) ==
      MetricTypeJson.TIMER);
    Assert.assertTrue(BeanHelper.unwrapMetricType(MetricTypeJson.TIMER) ==
      MetricType.TIMER);
  }
}
