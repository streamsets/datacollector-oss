/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.LocaleInContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TestPipelineDefinitionLocalization {

  @After
  public void cleanUp() {
    LocaleInContext.set(null);
  }


  private void testMessages(Locale locale, Map<String, String> expected) {
    PipelineDefinition def = new PipelineDefinition();
    LocaleInContext.set(locale);
    def = def.localize();

    //pipeline groups
    Assert.assertEquals(expected.get("pipelineConstantsGroup"), def.getConfigGroupDefinition().
      getGroupNameToLabelMapList().get(0).get("label"));

    Assert.assertEquals(expected.get("pipelineErrorRecordsGroup"), def.getConfigGroupDefinition().
      getGroupNameToLabelMapList().get(1).get("label"));

    //pipeline configs
    Assert.assertEquals(expected.get("deliveryGuaranteeLabel"), def.getConfigDefinitions().get(0).getLabel());
    Assert.assertEquals(expected.get("deliveryGuaranteeDescription"),
                        def.getConfigDefinitions().get(0).getDescription());
    Assert.assertEquals(expected.get("badRecordsHandlingLabel"), def.getConfigDefinitions().get(1).getLabel());
    Assert.assertEquals(expected.get("badRecordsHandlingDescription"),
                        def.getConfigDefinitions().get(1).getDescription());

  }

  private static final Map<String, String> EXPECTED_BUILT_IN = new HashMap<>();

  static {
    EXPECTED_BUILT_IN.put("pipelineConstantsGroup", PipelineDefConfigs.Groups.CONSTANTS.getLabel());
    EXPECTED_BUILT_IN.put("pipelineErrorRecordsGroup", PipelineDefConfigs.Groups.BAD_RECORDS.getLabel());
    EXPECTED_BUILT_IN.put("deliveryGuaranteeLabel", "Delivery Guarantee");
    EXPECTED_BUILT_IN.put("deliveryGuaranteeDescription", "");
    EXPECTED_BUILT_IN.put("badRecordsHandlingLabel", "Error Records");
    EXPECTED_BUILT_IN.put("badRecordsHandlingDescription", "");
  }

  @Test
  public void testBuiltInMessages() throws Exception {
    testMessages(Locale.getDefault(), EXPECTED_BUILT_IN);
  }

  private static final Map<String, String> EXPECTED_RESOURCE_BUNDLE = new HashMap<>();

  static {
    EXPECTED_RESOURCE_BUNDLE.put("pipelineConstantsGroup", "XConstants");
    EXPECTED_RESOURCE_BUNDLE.put("pipelineErrorRecordsGroup", "XError Records");
    EXPECTED_RESOURCE_BUNDLE.put("deliveryGuaranteeLabel", "XDelivery Guarantee");
    EXPECTED_RESOURCE_BUNDLE.put("deliveryGuaranteeDescription", "X");
    EXPECTED_RESOURCE_BUNDLE.put("badRecordsHandlingLabel", "XError Records");
    EXPECTED_RESOURCE_BUNDLE.put("badRecordsHandlingDescription", "X");
  }

  @Test
  public void testResourceBundleMessages() throws Exception {
    testMessages(new Locale("xyz"), EXPECTED_RESOURCE_BUNDLE);
  }

}
