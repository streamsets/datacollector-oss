/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import org.junit.Assert;
import org.junit.Test;

public class TestMetricRuleDefinitionBean {

  @Test(expected = NullPointerException.class)
  public void testMetricRuleDefinitionBeanNull() {
    MetricsRuleDefinitionJson metricsRuleDefinitionJson = new MetricsRuleDefinitionJson(null);
  }

  @Test
  public void testMetricRuleDefinitionBean() {
    com.streamsets.pipeline.config.MetricsRuleDefinition metricsRuleDefinition =
      new com.streamsets.pipeline.config.MetricsRuleDefinition("id", "Alert", "mId", MetricType.METER,
        MetricElement.METER_H1_RATE, "condition", true, true);

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
    com.streamsets.pipeline.config.MetricsRuleDefinition metricsRuleDefinition =
      new com.streamsets.pipeline.config.MetricsRuleDefinition("id", "Alert", "mId", MetricType.METER,
        MetricElement.METER_H1_RATE, "condition", true, true);

    MetricsRuleDefinitionJson metricsRuleDefinitionJsonBean = new MetricsRuleDefinitionJson("id", "Alert", "mId",
      MetricTypeJson.METER,
      MetricElementJson.METER_H1_RATE, "condition", true, true);

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
