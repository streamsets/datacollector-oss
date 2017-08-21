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

public class TestDataRuleDefinitionBean {

  @Test(expected = NullPointerException.class)
  public void testDataRuleDefinitionBeanNull() {
    DataRuleDefinitionJson dataRuleDefinitionJson = new DataRuleDefinitionJson(null);
  }

  @Test(expected = NullPointerException.class)
  public void testDriftRuleDefinitionBeanNull() {
    new DriftRuleDefinitionJson(null);
  }

  @Test
  public void testDataRuleDefinitionBean() {
    com.streamsets.datacollector.config.DataRuleDefinition dataRuleDefinition =
      new com.streamsets.datacollector.config.DataRuleDefinition("nameNotNull","nameNotNull", "lane", 100, 10,
        "${record:value(\"/name\")==null}", true, "nameNotNull", com.streamsets.datacollector.config.ThresholdType.COUNT,
        "2", 5, true, false, true, System.currentTimeMillis());

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
  public void testDriftRuleDefinitionBean() {
    com.streamsets.datacollector.config.DriftRuleDefinition driftRuleDefinition =
        new com.streamsets.datacollector.config.DriftRuleDefinition("nameNotNull","nameNotNull", "lane", 100, 10,
            "${record:value(\"/name\")==null}", true, "nameNotNull", true, false, true, System.currentTimeMillis());

    DriftRuleDefinitionJson driftRuleDefinitionJsonBean = new DriftRuleDefinitionJson(driftRuleDefinition);

    Assert.assertEquals(driftRuleDefinition.getLabel(), driftRuleDefinitionJsonBean.getLabel());
    Assert.assertEquals(driftRuleDefinition.getLane(), driftRuleDefinitionJsonBean.getLane());
    Assert.assertTrue(driftRuleDefinition.getSamplingPercentage() == driftRuleDefinitionJsonBean.getSamplingPercentage());
    Assert.assertEquals(driftRuleDefinition.getSamplingRecordsToRetain(),
        driftRuleDefinitionJsonBean.getSamplingRecordsToRetain());
    Assert.assertEquals(driftRuleDefinition.getAlertText(), driftRuleDefinitionJsonBean.getAlertText());
    Assert.assertEquals(driftRuleDefinition.getId(), driftRuleDefinitionJsonBean.getId());
    Assert.assertEquals(driftRuleDefinition.getCondition(), driftRuleDefinitionJsonBean.getCondition());
    Assert.assertEquals(driftRuleDefinition.isAlertEnabled(), driftRuleDefinitionJsonBean.isAlertEnabled());
    Assert.assertEquals(driftRuleDefinition.isMeterEnabled(), driftRuleDefinitionJsonBean.isMeterEnabled());
    Assert.assertEquals(driftRuleDefinition.isSendEmail(), driftRuleDefinitionJsonBean.isSendEmail());
    Assert.assertEquals(driftRuleDefinition.isValid(), driftRuleDefinitionJsonBean.isValid());
    Assert.assertEquals(driftRuleDefinition.isEnabled(), driftRuleDefinitionJsonBean.isEnabled());
  }

  @Test
  public void testDataRuleDefinitionBeanConstructorWithArgs() {
    com.streamsets.datacollector.config.DataRuleDefinition dataRuleDefinition =
      new com.streamsets.datacollector.config.DataRuleDefinition("nameNotNull","nameNotNull", "lane", 100, 10,
        "${record:value(\"/name\")==null}", true, "nameNotNull", com.streamsets.datacollector.config.ThresholdType.COUNT,
        "2", 5, true, false, true, System.currentTimeMillis());

    DataRuleDefinitionJson dataRuleDefinitionJsonBean = new DataRuleDefinitionJson("nameNotNull","nameNotNull", "lane", 100, 10,
      "${record:value(\"/name\")==null}", true, "nameNotNull", ThresholdTypeJson.COUNT, "2", 5, true, false, true,
      System.currentTimeMillis());

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

  @Test
  public void testDriftRuleDefinitionBeanConstructorWithArgs() {
    com.streamsets.datacollector.config.DriftRuleDefinition DriftRuleDefinition =
        new com.streamsets.datacollector.config.DriftRuleDefinition("nameNotNull","nameNotNull", "lane", 100, 10,
            "${record:value(\"/name\")==null}", true, "nameNotNull", true, false, true, System.currentTimeMillis());

    DriftRuleDefinitionJson DriftRuleDefinitionJsonBean = new DriftRuleDefinitionJson("nameNotNull","nameNotNull", "lane", 100, 10,
        "${record:value(\"/name\")==null}", true, "nameNotNull", true, false, true, System.currentTimeMillis());

    Assert.assertEquals(DriftRuleDefinition.getLabel(), DriftRuleDefinitionJsonBean.getLabel());
    Assert.assertEquals(DriftRuleDefinition.getLane(), DriftRuleDefinitionJsonBean.getLane());
    Assert.assertTrue(DriftRuleDefinition.getSamplingPercentage() == DriftRuleDefinitionJsonBean.getSamplingPercentage());
    Assert.assertEquals(DriftRuleDefinition.getSamplingRecordsToRetain(),
        DriftRuleDefinitionJsonBean.getSamplingRecordsToRetain());
    Assert.assertEquals(DriftRuleDefinition.getAlertText(), DriftRuleDefinitionJsonBean.getAlertText());
    Assert.assertEquals(DriftRuleDefinition.getId(), DriftRuleDefinitionJsonBean.getId());
    Assert.assertEquals(DriftRuleDefinition.getCondition(), DriftRuleDefinitionJsonBean.getCondition());
    Assert.assertEquals(DriftRuleDefinition.isAlertEnabled(), DriftRuleDefinitionJsonBean.isAlertEnabled());
    Assert.assertEquals(DriftRuleDefinition.isMeterEnabled(), DriftRuleDefinitionJsonBean.isMeterEnabled());
    Assert.assertEquals(DriftRuleDefinition.isSendEmail(), DriftRuleDefinitionJsonBean.isSendEmail());
    Assert.assertEquals(DriftRuleDefinition.isValid(), DriftRuleDefinitionJsonBean.isValid());
    Assert.assertEquals(DriftRuleDefinition.isEnabled(), DriftRuleDefinitionJsonBean.isEnabled());

    //underlying DriftRuleDefinition
    Assert.assertEquals(DriftRuleDefinition.getLabel(), DriftRuleDefinitionJsonBean.getDriftRuleDefinition().getLabel());
    Assert.assertEquals(DriftRuleDefinition.getLane(), DriftRuleDefinitionJsonBean.getDriftRuleDefinition().getLane());
    Assert.assertEquals(DriftRuleDefinition.getMinVolume(),
        DriftRuleDefinitionJsonBean.getDriftRuleDefinition().getMinVolume());
    Assert.assertTrue(DriftRuleDefinition.getSamplingPercentage() ==
        DriftRuleDefinitionJsonBean.getDriftRuleDefinition().getSamplingPercentage());
    Assert.assertEquals(DriftRuleDefinition.getSamplingRecordsToRetain(),
        DriftRuleDefinitionJsonBean.getDriftRuleDefinition().getSamplingRecordsToRetain());
    Assert.assertEquals(DriftRuleDefinition.getThresholdType(),
        DriftRuleDefinitionJsonBean.getDriftRuleDefinition().getThresholdType());
    Assert.assertEquals(DriftRuleDefinition.getThresholdValue(),
        DriftRuleDefinitionJsonBean.getDriftRuleDefinition().getThresholdValue());
    Assert.assertEquals(DriftRuleDefinition.getAlertText(),
        DriftRuleDefinitionJsonBean.getDriftRuleDefinition().getAlertText());
    Assert.assertEquals(DriftRuleDefinition.getId(), DriftRuleDefinitionJsonBean.getDriftRuleDefinition().getId());
    Assert.assertEquals(DriftRuleDefinition.getCondition(),
        DriftRuleDefinitionJsonBean.getDriftRuleDefinition().getCondition());
    Assert.assertEquals(DriftRuleDefinition.isAlertEnabled(),
        DriftRuleDefinitionJsonBean.getDriftRuleDefinition().isAlertEnabled());
    Assert.assertEquals(DriftRuleDefinition.isMeterEnabled(),
        DriftRuleDefinitionJsonBean.getDriftRuleDefinition().isMeterEnabled());
    Assert.assertEquals(DriftRuleDefinition.isSendEmail(), DriftRuleDefinitionJsonBean.getDriftRuleDefinition().isSendEmail());
    Assert.assertEquals(DriftRuleDefinition.isValid(), DriftRuleDefinitionJsonBean.getDriftRuleDefinition().isValid());
    Assert.assertEquals(DriftRuleDefinition.isEnabled(), DriftRuleDefinitionJsonBean.getDriftRuleDefinition().isEnabled());
  }
}
