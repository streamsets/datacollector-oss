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

import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import org.junit.Assert;
import org.junit.Test;

public class TestMetricRuleDefinitionBean {

  @Test(expected = NullPointerException.class)
  public void testMetricRuleDefinitionBeanNull() {
    MetricsRuleDefinitionJson metricsRuleDefinitionJson = new MetricsRuleDefinitionJson(null);
  }

  @Test
  public void testMetricRuleDefinitionBean() {
    com.streamsets.datacollector.config.MetricsRuleDefinition metricsRuleDefinition =
      new com.streamsets.datacollector.config.MetricsRuleDefinition("id", "Alert", "mId", MetricType.METER,
        MetricElement.METER_H1_RATE, "condition", true, true, System.currentTimeMillis());

    MetricsRuleDefinitionJson metricsRuleDefinitionJsonBean = new MetricsRuleDefinitionJson(metricsRuleDefinition);

    Assert.assertEquals(metricsRuleDefinition.isEnabled(), metricsRuleDefinitionJsonBean.isEnabled());
    Assert.assertEquals(metricsRuleDefinition.isValid(), metricsRuleDefinitionJsonBean.isValid());
    Assert.assertEquals(metricsRuleDefinition.isSendEmail(), metricsRuleDefinitionJsonBean.isSendEmail());
    Assert.assertEquals(metricsRuleDefinition.getAlertText(), metricsRuleDefinitionJsonBean.getAlertText());
    Assert.assertEquals(metricsRuleDefinition.getCondition(), metricsRuleDefinitionJsonBean.getCondition());
    Assert.assertEquals(metricsRuleDefinition.getId(), metricsRuleDefinitionJsonBean.getId());
    Assert.assertEquals(metricsRuleDefinition.getMetricElement(),
      BeanHelper.unwrapMetricElement(metricsRuleDefinitionJsonBean.getMetricElement()));
    Assert.assertEquals(metricsRuleDefinition.getMetricType(),
      BeanHelper.unwrapMetricType(metricsRuleDefinitionJsonBean.getMetricType()));
    Assert.assertEquals(metricsRuleDefinition.getMetricId(), metricsRuleDefinitionJsonBean.getMetricId());

  }

  @Test
  public void testMetricRuleDefinitionBeanConstructorWithArgs() {
    com.streamsets.datacollector.config.MetricsRuleDefinition metricsRuleDefinition =
      new com.streamsets.datacollector.config.MetricsRuleDefinition("id", "Alert", "mId", MetricType.METER,
        MetricElement.METER_H1_RATE, "condition", true, true, System.currentTimeMillis());

    MetricsRuleDefinitionJson metricsRuleDefinitionJsonBean = new MetricsRuleDefinitionJson("id", "Alert", "mId",
      MetricTypeJson.METER,
      MetricElementJson.METER_H1_RATE, "condition", true, true, System.currentTimeMillis());

    Assert.assertEquals(metricsRuleDefinition.isEnabled(), metricsRuleDefinitionJsonBean.isEnabled());
    Assert.assertEquals(metricsRuleDefinition.isValid(), metricsRuleDefinitionJsonBean.isValid());
    Assert.assertEquals(metricsRuleDefinition.isSendEmail(), metricsRuleDefinitionJsonBean.isSendEmail());
    Assert.assertEquals(metricsRuleDefinition.getAlertText(), metricsRuleDefinitionJsonBean.getAlertText());
    Assert.assertEquals(metricsRuleDefinition.getCondition(), metricsRuleDefinitionJsonBean.getCondition());
    Assert.assertEquals(metricsRuleDefinition.getId(), metricsRuleDefinitionJsonBean.getId());
    Assert.assertEquals(metricsRuleDefinition.getMetricElement(),
      BeanHelper.unwrapMetricElement(metricsRuleDefinitionJsonBean.getMetricElement()));
    Assert.assertEquals(metricsRuleDefinition.getMetricType(),
      BeanHelper.unwrapMetricType(metricsRuleDefinitionJsonBean.getMetricType()));
    Assert.assertEquals(metricsRuleDefinition.getMetricId(), metricsRuleDefinitionJsonBean.getMetricId());

    //test underlying
    Assert.assertEquals(metricsRuleDefinition.isEnabled(),
      metricsRuleDefinitionJsonBean.getMetricsRuleDefinition().isEnabled());
    Assert.assertEquals(metricsRuleDefinition.isValid(),
      metricsRuleDefinitionJsonBean.getMetricsRuleDefinition().isValid());
    Assert.assertEquals(metricsRuleDefinition.isSendEmail(),
      metricsRuleDefinitionJsonBean.getMetricsRuleDefinition().isSendEmail());
    Assert.assertEquals(metricsRuleDefinition.getAlertText(),
      metricsRuleDefinitionJsonBean.getMetricsRuleDefinition().getAlertText());
    Assert.assertEquals(metricsRuleDefinition.getCondition(),
      metricsRuleDefinitionJsonBean.getMetricsRuleDefinition().getCondition());
    Assert.assertEquals(metricsRuleDefinition.getId(),
      metricsRuleDefinitionJsonBean.getMetricsRuleDefinition().getId());
    Assert.assertEquals(metricsRuleDefinition.getMetricElement(),
      metricsRuleDefinitionJsonBean.getMetricsRuleDefinition().getMetricElement());
    Assert.assertEquals(metricsRuleDefinition.getMetricType(),
      metricsRuleDefinitionJsonBean.getMetricsRuleDefinition().getMetricType());
    Assert.assertEquals(metricsRuleDefinition.getMetricId(),
      metricsRuleDefinitionJsonBean.getMetricsRuleDefinition().getMetricId());
  }
}
