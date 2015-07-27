/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import org.junit.Assert;
import org.junit.Test;

import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.DataRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.ThresholdTypeJson;

public class TestDataRuleDefinitionBean {

  @Test(expected = NullPointerException.class)
  public void testDataRuleDefinitionBeanNull() {
    DataRuleDefinitionJson dataRuleDefinitionJson = new DataRuleDefinitionJson(null);
  }

  @Test
  public void testDataRuleDefinitionBean() {
    com.streamsets.datacollector.config.DataRuleDefinition dataRuleDefinition =
      new com.streamsets.datacollector.config.DataRuleDefinition("nameNotNull","nameNotNull", "lane", 100, 10,
        "${record:value(\"/name\")==null}", true, "nameNotNull", com.streamsets.datacollector.config.ThresholdType.COUNT,
        "2", 5, true, false, true);

    DataRuleDefinitionJson dataRuleDefinitionJsonBean = new DataRuleDefinitionJson(dataRuleDefinition);

    Assert.assertEquals(dataRuleDefinition.getLabel(), dataRuleDefinitionJsonBean.getLabel());
    Assert.assertEquals(dataRuleDefinition.getLane(), dataRuleDefinitionJsonBean.getLane());
    Assert.assertEquals(dataRuleDefinition.getMinVolume(), dataRuleDefinitionJsonBean.getMinVolume());
    Assert.assertTrue(dataRuleDefinition.getSamplingPercentage() == dataRuleDefinitionJsonBean.getSamplingPercentage());
    Assert.assertEquals(dataRuleDefinition.getSamplingRecordsToRetain(),
      dataRuleDefinitionJsonBean.getSamplingRecordsToRetain());
    Assert.assertEquals(dataRuleDefinition.getThresholdType(),
      BeanHelper.unwrapThresholdType(dataRuleDefinitionJsonBean.getThresholdType()));
    Assert.assertEquals(dataRuleDefinition.getThresholdValue(), dataRuleDefinitionJsonBean.getThresholdValue());
    Assert.assertEquals(dataRuleDefinition.getAlertText(), dataRuleDefinitionJsonBean.getAlertText());
    Assert.assertEquals(dataRuleDefinition.getId(), dataRuleDefinitionJsonBean.getId());
    Assert.assertEquals(dataRuleDefinition.getCondition(), dataRuleDefinitionJsonBean.getCondition());
    Assert.assertEquals(dataRuleDefinition.isAlertEnabled(), dataRuleDefinitionJsonBean.isAlertEnabled());
    Assert.assertEquals(dataRuleDefinition.isMeterEnabled(), dataRuleDefinitionJsonBean.isMeterEnabled());
    Assert.assertEquals(dataRuleDefinition.isSendEmail(), dataRuleDefinitionJsonBean.isSendEmail());
    Assert.assertEquals(dataRuleDefinition.isValid(), dataRuleDefinitionJsonBean.isValid());
    Assert.assertEquals(dataRuleDefinition.isEnabled(), dataRuleDefinitionJsonBean.isEnabled());
  }

  @Test
  public void testDataRuleDefinitionBeanConstructorWithArgs() {
    com.streamsets.datacollector.config.DataRuleDefinition dataRuleDefinition =
      new com.streamsets.datacollector.config.DataRuleDefinition("nameNotNull","nameNotNull", "lane", 100, 10,
        "${record:value(\"/name\")==null}", true, "nameNotNull", com.streamsets.datacollector.config.ThresholdType.COUNT,
        "2", 5, true, false, true);

    DataRuleDefinitionJson dataRuleDefinitionJsonBean = new DataRuleDefinitionJson("nameNotNull","nameNotNull", "lane", 100, 10,
      "${record:value(\"/name\")==null}", true, "nameNotNull", ThresholdTypeJson.COUNT, "2", 5, true, false, true);

    Assert.assertEquals(dataRuleDefinition.getLabel(), dataRuleDefinitionJsonBean.getLabel());
    Assert.assertEquals(dataRuleDefinition.getLane(), dataRuleDefinitionJsonBean.getLane());
    Assert.assertEquals(dataRuleDefinition.getMinVolume(), dataRuleDefinitionJsonBean.getMinVolume());
    Assert.assertTrue(dataRuleDefinition.getSamplingPercentage() == dataRuleDefinitionJsonBean.getSamplingPercentage());
    Assert.assertEquals(dataRuleDefinition.getSamplingRecordsToRetain(),
      dataRuleDefinitionJsonBean.getSamplingRecordsToRetain());
    Assert.assertEquals(dataRuleDefinition.getThresholdType(),
      BeanHelper.unwrapThresholdType(dataRuleDefinitionJsonBean.getThresholdType()));
    Assert.assertEquals(dataRuleDefinition.getThresholdValue(), dataRuleDefinitionJsonBean.getThresholdValue());
    Assert.assertEquals(dataRuleDefinition.getAlertText(), dataRuleDefinitionJsonBean.getAlertText());
    Assert.assertEquals(dataRuleDefinition.getId(), dataRuleDefinitionJsonBean.getId());
    Assert.assertEquals(dataRuleDefinition.getCondition(), dataRuleDefinitionJsonBean.getCondition());
    Assert.assertEquals(dataRuleDefinition.isAlertEnabled(), dataRuleDefinitionJsonBean.isAlertEnabled());
    Assert.assertEquals(dataRuleDefinition.isMeterEnabled(), dataRuleDefinitionJsonBean.isMeterEnabled());
    Assert.assertEquals(dataRuleDefinition.isSendEmail(), dataRuleDefinitionJsonBean.isSendEmail());
    Assert.assertEquals(dataRuleDefinition.isValid(), dataRuleDefinitionJsonBean.isValid());
    Assert.assertEquals(dataRuleDefinition.isEnabled(), dataRuleDefinitionJsonBean.isEnabled());

    //underlying DataRuleDefinition
    Assert.assertEquals(dataRuleDefinition.getLabel(), dataRuleDefinitionJsonBean.getDataRuleDefinition().getLabel());
    Assert.assertEquals(dataRuleDefinition.getLane(), dataRuleDefinitionJsonBean.getDataRuleDefinition().getLane());
    Assert.assertEquals(dataRuleDefinition.getMinVolume(),
      dataRuleDefinitionJsonBean.getDataRuleDefinition().getMinVolume());
    Assert.assertTrue(dataRuleDefinition.getSamplingPercentage() ==
      dataRuleDefinitionJsonBean.getDataRuleDefinition().getSamplingPercentage());
    Assert.assertEquals(dataRuleDefinition.getSamplingRecordsToRetain(),
      dataRuleDefinitionJsonBean.getDataRuleDefinition().getSamplingRecordsToRetain());
    Assert.assertEquals(dataRuleDefinition.getThresholdType(),
      dataRuleDefinitionJsonBean.getDataRuleDefinition().getThresholdType());
    Assert.assertEquals(dataRuleDefinition.getThresholdValue(),
      dataRuleDefinitionJsonBean.getDataRuleDefinition().getThresholdValue());
    Assert.assertEquals(dataRuleDefinition.getAlertText(),
      dataRuleDefinitionJsonBean.getDataRuleDefinition().getAlertText());
    Assert.assertEquals(dataRuleDefinition.getId(), dataRuleDefinitionJsonBean.getDataRuleDefinition().getId());
    Assert.assertEquals(dataRuleDefinition.getCondition(),
      dataRuleDefinitionJsonBean.getDataRuleDefinition().getCondition());
    Assert.assertEquals(dataRuleDefinition.isAlertEnabled(),
      dataRuleDefinitionJsonBean.getDataRuleDefinition().isAlertEnabled());
    Assert.assertEquals(dataRuleDefinition.isMeterEnabled(),
      dataRuleDefinitionJsonBean.getDataRuleDefinition().isMeterEnabled());
    Assert.assertEquals(dataRuleDefinition.isSendEmail(), dataRuleDefinitionJsonBean.getDataRuleDefinition().isSendEmail());
    Assert.assertEquals(dataRuleDefinition.isValid(), dataRuleDefinitionJsonBean.getDataRuleDefinition().isValid());
    Assert.assertEquals(dataRuleDefinition.isEnabled(), dataRuleDefinitionJsonBean.getDataRuleDefinition().isEnabled());
  }
}
