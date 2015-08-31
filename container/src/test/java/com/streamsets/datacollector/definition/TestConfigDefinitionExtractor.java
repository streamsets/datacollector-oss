/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.definition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.PredicateModel;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestConfigDefinitionExtractor {

  public static class Empty {
  }

  public static class ELs {

    @ElFunction(prefix = "p", name = "f", description = "ff")
    public static String f(@ElParam("x") int x) {
      return null;
    }

    @ElConstant(name = "C", description = "CC")
    public static final String C = "c";

  }

  public static class Ok1 {

    @ConfigDef(
        label = "L",
        description = "D",
        type = ConfigDef.Type.NUMBER,
        defaultValue = "10",
        required = true,
        group = "G",
        displayPosition = 1,
        lines = 2,
        min = 3,
        max = 4,
        evaluation = ConfigDef.Evaluation.EXPLICIT,
        mode = ConfigDef.Mode.JAVA,
        elDefs = ELs.class
    )
    public int config;
  }

  public static class Ok2 {

    @ConfigDef(
        label = "L",
        description = "D",
        type = ConfigDef.Type.BOOLEAN,
        required = true,
        group = "G",
        displayPosition = 1,
        lines = 2,
        evaluation = ConfigDef.Evaluation.EXPLICIT,
        mode = ConfigDef.Mode.JAVA,
        elDefs = ELs.class
    )
    public String config;
  }

  public static class Ok3 {

    @ConfigDef(
        label = "L",
        description = "D",
        type = ConfigDef.Type.MODEL,
        required = true,
        elDefs = ELs.class
    )
    @PredicateModel
    public List<String> predicates;
  }

  public static class Ok4 {

    @ConfigDef(
        label = "L",
        description = "D",
        type = ConfigDef.Type.MODEL,
        required = true,
        elDefs = ELs.class
    )
    @FieldSelectorModel
    public String selector;
  }

  @Test
  public void testStringConfigOk() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(Ok1.class, Collections.<String>emptyList(),
                                                                             "x");
    Assert.assertEquals(1, configs.size());
    ConfigDefinition config = configs.get(0);
    Assert.assertEquals("config", config.getName());
    Assert.assertEquals("config", config.getFieldName());
    Assert.assertEquals(ConfigDef.Type.NUMBER, config.getType());
    Assert.assertEquals("L", config.getLabel());
    Assert.assertEquals("D", config.getDescription());
    Assert.assertEquals(10, config.getDefaultValue());
    Assert.assertEquals(true, config.isRequired());
    Assert.assertEquals("G", config.getGroup());
    Assert.assertEquals(1, config.getDisplayPosition());
    Assert.assertEquals(2, config.getLines());
    Assert.assertEquals(3, config.getMin());
    Assert.assertEquals(4, config.getMax());
    Assert.assertEquals(ConfigDef.Evaluation.EXPLICIT, config.getEvaluation());
    Assert.assertEquals("text/x-java", config.getMode());
    Assert.assertTrue(containsF(config.getElFunctionDefinitions(), "p:f"));
    Assert.assertTrue(containsC(config.getElConstantDefinitions(), "C"));
    Assert.assertNull(config.getModel());
    Assert.assertEquals("", config.getDependsOn());
    Assert.assertNull(config.getTriggeredByValues());
  }

  @Test
  public void testTypeNoELConfigOk() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(Ok2.class, Collections.<String>emptyList(),
                                                                             "x");
    Assert.assertEquals(1, configs.size());
    ConfigDefinition config = configs.get(0);
    Assert.assertEquals(ConfigDef.Type.BOOLEAN, config.getType());
    Assert.assertNull(config.getDefaultValue());
    Assert.assertTrue(config.getElFunctionDefinitions().isEmpty());
    Assert.assertTrue(config.getElConstantDefinitions().isEmpty());
  }

  @Test
  public void testModelNoELConfigOk() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(Ok4.class, Collections.<String>emptyList(),
                                                                             "x");
    Assert.assertEquals(1, configs.size());
    ConfigDefinition config = configs.get(0);
    Assert.assertEquals(ConfigDef.Type.MODEL, config.getType());
    Assert.assertTrue(config.getElFunctionDefinitions().isEmpty());
    Assert.assertTrue(config.getElConstantDefinitions().isEmpty());
  }

  @Test
  public void testModelELConfigOk() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(Ok3.class, Collections.<String>emptyList(),
                                                                             "x");
    Assert.assertEquals(1, configs.size());
    ConfigDefinition config = configs.get(0);
    Assert.assertEquals(ConfigDef.Type.MODEL, config.getType());
    Assert.assertFalse(config.getElFunctionDefinitions().isEmpty());
    Assert.assertFalse(config.getElConstantDefinitions().isEmpty());
  }

  private boolean containsF(List<ElFunctionDefinition> defs, String name) {
    for (ElFunctionDefinition def : defs) {
      if (def.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  private boolean containsC(List<ElConstantDefinition> defs, String name) {
    for (ElConstantDefinition def : defs) {
      if (def.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }


  public static class Model {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @FieldSelectorModel
    public String config;
  }

  @Test
  public void testModel() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(Model.class, Collections.<String>emptyList(),
                                                                             "x");
    Assert.assertEquals(1, configs.size());
    ConfigDefinition config = configs.get(0);
    Assert.assertEquals(ConfigDef.Type.MODEL, config.getType());
    Assert.assertNotNull(config.getModel());
  }

  public static class Fail1 {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = false
    )
    private String config;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStringConfigFail1() {
    ConfigDefinitionExtractor.get().extract(Fail1.class, Collections.<String>emptyList(), "x");
  }

  public static class Fail2 {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = false
    )
    public static String config;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStringConfigFail2() {
    ConfigDefinitionExtractor.get().extract(Fail2.class, Collections.<String>emptyList(), "x");
  }

  public static class Fail3 {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = false,
        min = 0
    )
    public static String config;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStringConfigFail3() {
    ConfigDefinitionExtractor.get().extract(Fail3.class, Collections.<String>emptyList(), "x");
  }

  public static class DependsOn {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependsOn = "b",
        triggeredByValue = "B"
    )
    public String a;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true
    )
    public String b;
  }

  @Test
  public void testDependsOn() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(DependsOn.class,
                                                                             Collections.<String>emptyList(),"x");
    Assert.assertEquals(2, configs.size());
    ConfigDefinition cd1 = configs.get(0);
    ConfigDefinition cd2 = configs.get(1);
    ConfigDefinition a;
    ConfigDefinition b;
    if (cd1.getName().equals("a")) {
      a = cd1;
      b = cd2;
    } else {
      a = cd2;
      b = cd1;
    }
    Assert.assertEquals("b", a.getDependsOn());
    Assert.assertEquals(ImmutableList.of("B"), a.getTriggeredByValues());
    Assert.assertEquals(1, a.getDependsOnMap().size());
    Assert.assertEquals(ImmutableList.of("B"), a.getDependsOnMap().get("b"));
    Assert.assertEquals("", b.getDependsOn());
    Assert.assertNull(b.getTriggeredByValues());
  }

  public static class DependsOnChain {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependsOn = "b",
        triggeredByValue = "B"
    )
    public String a;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependsOn = "c",
        triggeredByValue = "C"
    )
    public String b;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true
    )
    public String c;
  }

  @Test
  public void testDependsOnChain() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(DependsOnChain.class,
                                                                             Collections.<String>emptyList(), "x");
    Assert.assertEquals(3, configs.size());
    ConfigDefinition a = null;
    for (ConfigDefinition cd : configs) {
      if (cd.getName().equals("a")) {
        a = cd;
      }
    }
    Assert.assertNotNull(a);
    Assert.assertEquals("b", a.getDependsOn());
    Assert.assertEquals(ImmutableList.of("B"), a.getTriggeredByValues());
    Assert.assertEquals(2, a.getDependsOnMap().size());
    Assert.assertEquals(ImmutableList.of("B"), a.getDependsOnMap().get("b"));
    Assert.assertEquals(ImmutableList.of("C"), a.getDependsOnMap().get("c"));
  }

  public static class SubSubBean {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.NUMBER    )
    public long prop5;

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.NUMBER,
        dependsOn = "prop5",
        triggeredByValue = "1",
        group = "foo"
    )
    public long prop6;

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.NUMBER,
        dependsOn = "^prop2.prop3",
        triggeredByValue = "2",
        group = "#1"
    )
    public long prop7;

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.NUMBER,
        dependsOn = "prop1^^",
        triggeredByValue = "3"
    )
    public long prop8;

  }

  public static class SubBean {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.NUMBER,
        group = "foo"
    )
    public long prop3;

    @ConfigDefBean
    public SubSubBean prop4;

  }

  public static class Bean {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING
    )
    public String prop1;

    @ConfigDefBean(groups = { "foo", "#0"})
    public SubBean prop2;
  }

  @Test
  public void testConfigDefBean() {
    Assert.assertTrue(ConfigDefinitionExtractor.get().validate(Bean.class, ImmutableList.of("bar", "foo"),
                                                               "x").isEmpty());
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(Bean.class,
                                                                             ImmutableList.of("bar", "foo"), "x");
    Assert.assertEquals(6, configs.size());
    Set<String> expectedNames = ImmutableSet.of("prop1", "prop2.prop3", "prop2.prop4.prop5", "prop2.prop4.prop6",
                                                "prop2.prop4.prop7", "prop2.prop4.prop8");
    Set<String> expectedFieldNames = ImmutableSet.of("prop1", "prop3", "prop5", "prop6", "prop7", "prop8");
    Set<String> gotNames = new HashSet<>();
    Set<String> gotFieldNames = new HashSet<>();
    for (ConfigDefinition cd : configs) {
      gotNames.add(cd.getName());
      gotFieldNames.add(cd.getFieldName());
    }
    Assert.assertEquals(expectedNames, gotNames);
    Assert.assertEquals(expectedFieldNames, gotFieldNames);
  }

  @Test
  public void testGroupsResolution() {
    Assert.assertTrue(ConfigDefinitionExtractor.get().validate(Bean.class, ImmutableList.of("bar", "foo"),
                                                               "x").isEmpty());
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(Bean.class,
                                                                             ImmutableList.of("bar", "foo"), "x");
    Map<String, ConfigDefinition> configMap = new HashMap<>();
    for (ConfigDefinition config : configs) {
      configMap.put(config.getName(), config);
    }
    Assert.assertEquals("", configMap.get("prop1").getGroup());
    Assert.assertEquals("foo", configMap.get("prop2.prop3").getGroup());
    Assert.assertEquals("", configMap.get("prop2.prop4.prop5").getGroup());
    Assert.assertEquals("foo", configMap.get("prop2.prop4.prop6").getGroup());
    Assert.assertEquals("bar", configMap.get("prop2.prop4.prop7").getGroup());
  }

  public static class SubBean1 {

    public String prop4;

  }

  public static class Bean1 {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING
    )
    public String prop1;

    @ConfigDefBean
    public SubBean1 prop2;
  }

  @Test
  public void testConfigDefBeanInvalidBeanWithoutConfig() {
    Assert.assertEquals(1, ConfigDefinitionExtractor.get().validate(Bean1.class, Collections.<String>emptyList(),
                                                                    "x").size());
  }

  public static class SubBean2 {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING,
        dependsOn = "props^^"
    )
    public String prop3;

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING,
        dependsOn = "^props^^"
    )
    public String prop4;

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING,
        dependsOn = "props^3^"
    )
    public String prop5;

  }

  public static class Bean2 {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING
    )
    public String prop1;

    @ConfigDefBean
    public SubBean2 prop2;
  }

  @Test
  public void testConfigDefBeanInvalidDependsOn() {
    Assert.assertEquals(3, ConfigDefinitionExtractor.get().validate(Bean2.class, Collections.<String>emptyList(),
                                                                    "x").size());
  }

  public static class SubBean3 {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING,
        dependsOn = "props^"
    )
    public String prop3;

  }

  public static class Bean3 {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING
    )
    public String prop1;

    @ConfigDefBean
    public SubBean3 prop2;
  }

  @Test
  public void testConfigDefBeanNonExistentDependsOn() {
    Assert.assertEquals(1, ConfigDefinitionExtractor.get().validate(Bean3.class, Collections.<String>emptyList(),
                                                                    "x").size());
  }

}
