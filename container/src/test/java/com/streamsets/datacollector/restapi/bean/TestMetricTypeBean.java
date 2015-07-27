/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
