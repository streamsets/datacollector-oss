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
package com.streamsets.datacollector.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.runner.StageDefinitionBuilder;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.impl.LocaleInContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TestStageDefinitionLocalization {

  @After
  public void cleanUp() {
    LocaleInContext.set(null);
  }

  @SuppressWarnings("unchecked")
  private StageDefinition createStageDefinition() {
    List<ConfigDefinition> configs = new ArrayList<>();
    configs.add(new ConfigDefinition("c1", ConfigDef.Type.STRING,
        ConfigDef.Upload.NO, "Config1Label", "Config1Description", "default",
                                     true, "GROUP", "c1", null, null, null, 0,
                                     Collections.<ElFunctionDefinition>emptyList(),
                                     Collections.<ElConstantDefinition>emptyList(),
                                     0, 0, "mode", 1,
      Collections.<Class> emptyList(), ConfigDef.Evaluation.IMPLICIT, null, ConfigDef.DisplayMode.BASIC, ""));
    ModelDefinition model = new ModelDefinition(ModelType.VALUE_CHOOSER, OptionsChooserValues.class.getName(),
                                                ImmutableList.of("OPTION"), ImmutableList.of("Option"), null,  null, null);
    configs.add(new ConfigDefinition("c2", ConfigDef.Type.MODEL, ConfigDef.Upload.NO, "Config2Label", "Config2Description", "default",
                                     true, "GROUP", "c2", model, null, null, 0,
                                     Collections.<ElFunctionDefinition>emptyList(),
                                     Collections.<ElConstantDefinition>emptyList(),
                                     0, 0, "mode", 1,
      Collections.<Class> emptyList(), ConfigDef.Evaluation.IMPLICIT, null, ConfigDef.DisplayMode.BASIC, ""));
    RawSourceDefinition rawSource = new RawSourceDefinition(TRawSourcePreviewer.class.getName(), "*/*", configs);
    ConfigGroupDefinition configGroup = new ConfigGroupDefinition(ImmutableSet.of("GROUP"),
        (Map)ImmutableMap.of(Groups.class.getName(), ImmutableList.of(Groups.GROUP.name())),
        (List)ImmutableList.of(ImmutableMap.of("label", "Group", "name", "GROUP"))
    );
    StageDefinition def = new StageDefinitionBuilder(TestStageDefinitionLocalization.class.getClassLoader(), TProcessor.class, "stage")
      .withStageDef(Mockito.mock(StageDef.class))
      .withConfig(configs)
      .withErrorStage(true)
      .withRawSourceDefintion(rawSource)
      .withExecutionModes(ExecutionMode.CLUSTER_BATCH, ExecutionMode.STANDALONE)
      .withConfigGroupDefintion(configGroup)
      .withOutputStreamLabelProviderClass(TOutput.class.getName())
      .withLabel("StageLabel")
      .withDescription("StageDescription")
      .build();
    return def;
  }

  private void testMessages(Locale locale, Map<String, String> expected) {
    StageDefinition def = createStageDefinition();
    LocaleInContext.set(locale);
    def = def.localize();
    //stage
    Assert.assertEquals(expected.get("stageLabel"), def.getLabel());
    Assert.assertEquals(expected.get("stageDescription"), def.getDescription());

    //error stage
    Assert.assertTrue(def.isErrorStage());

    //stage groups
    Assert.assertEquals(expected.get("stageGroup"), def.getConfigGroupDefinition().getGroupNameToLabelMapList().get(0).get(
        "label"));

    //stage configs
    Assert.assertEquals(expected.get("c1Label"), def.getConfigDefinition("c1").getLabel());
    Assert.assertEquals(expected.get("c1Description"), def.getConfigDefinition("c1").getDescription());
    Assert.assertEquals(expected.get("c2Label"), def.getConfigDefinition("c2").getLabel());
    Assert.assertEquals(expected.get("c2Description"),def.getConfigDefinition("c2").getDescription());
    Assert.assertEquals(expected.get("c2OptionLabel" +
                                     ""),def.getConfigDefinition("c2").getModel().getLabels().get(0));

    //stage raw preview
    Assert.assertEquals(expected.get("c1Label"),
                        def.getRawSourceDefinition().getConfigDefinitions().get(0).getLabel());
    Assert.assertEquals(expected.get("c1Description"),
                        def.getRawSourceDefinition().getConfigDefinitions().get(0).getDescription());
    Assert.assertEquals(expected.get("c2Label"),
                        def.getRawSourceDefinition().getConfigDefinitions().get(1).getLabel());
    Assert.assertEquals(expected.get("c2Description"),
                        def.getRawSourceDefinition().getConfigDefinitions().get(1).getDescription());
    Assert.assertEquals(expected.get("c2OptionLabel"),
                        def.getRawSourceDefinition().getConfigDefinitions().get(1).getModel().getLabels().get(0));

    //stage output streams
    Assert.assertEquals(expected.get("stageOutput"), def.getOutputStreamLabels().get(0));
  }

  private static final Map<String, String> EXPECTED_BUILT_IN = new HashMap<>();

  static {
    EXPECTED_BUILT_IN.put("stageLabel", "StageLabel");
    EXPECTED_BUILT_IN.put("stageDescription", "StageDescription");
    EXPECTED_BUILT_IN.put("errorStageLabel", "ErrorStageLabel");
    EXPECTED_BUILT_IN.put("errorStageDescription", "ErrorStageDescription");
    EXPECTED_BUILT_IN.put("stageGroup", "Group");
    EXPECTED_BUILT_IN.put("c1Label", "Config1Label");
    EXPECTED_BUILT_IN.put("c1Description", "Config1Description");
    EXPECTED_BUILT_IN.put("c2Label", "Config2Label");
    EXPECTED_BUILT_IN.put("c2Description", "Config2Description");
    EXPECTED_BUILT_IN.put("c2OptionLabel", "Option");
    EXPECTED_BUILT_IN.put("stageOutput", "Output");
  }

  @Test
  public void testBuiltInMessages() throws Exception {
    testMessages(Locale.getDefault(), EXPECTED_BUILT_IN);
  }

  private static final Map<String, String> EXPECTED_RESOURCE_BUNDLE = new HashMap<>();

  static {
    EXPECTED_RESOURCE_BUNDLE.put("stageLabel", "XStageLabel");
    EXPECTED_RESOURCE_BUNDLE.put("stageDescription", "XStageDescription");
    EXPECTED_RESOURCE_BUNDLE.put("errorStageLabel", "XErrorStageLabel");
    EXPECTED_RESOURCE_BUNDLE.put("errorStageDescription", "XErrorStageDescription");
    EXPECTED_RESOURCE_BUNDLE.put("stageGroup", "XGroup");
    EXPECTED_RESOURCE_BUNDLE.put("c1Label", "XConfig1Label");
    EXPECTED_RESOURCE_BUNDLE.put("c1Description", "XConfig1Description");
    EXPECTED_RESOURCE_BUNDLE.put("c2Label", "XConfig2Label");
    EXPECTED_RESOURCE_BUNDLE.put("c2Description", "XConfig2Description");
    EXPECTED_RESOURCE_BUNDLE.put("c2OptionLabel", "XOption");
    EXPECTED_RESOURCE_BUNDLE.put("stageOutput", "XOutput");
  }

  @Test
  public void testResourceBundleMessages() throws Exception {
    testMessages(new Locale("xyz"), EXPECTED_RESOURCE_BUNDLE);
  }

}
