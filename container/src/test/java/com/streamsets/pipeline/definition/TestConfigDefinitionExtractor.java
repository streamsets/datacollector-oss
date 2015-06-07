/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.config.ConfigDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

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

  public static class Ok {

    @ConfigDef(
        label = "L",
        description = "D",
        type = ConfigDef.Type.STRING,
        defaultValue = "default",
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
    public String config;
  }

  @Test
  public void testStringConfigOk() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(Ok.class, "x");
    Assert.assertEquals(1, configs.size());
    ConfigDefinition config = configs.get(0);
    Assert.assertEquals("config", config.getName());
    Assert.assertEquals("config", config.getFieldName());
    Assert.assertEquals(ConfigDef.Type.STRING, config.getType());
    Assert.assertEquals("L", config.getLabel());
    Assert.assertEquals("D", config.getDescription());
    Assert.assertEquals("default", config.getDefaultValue());
    Assert.assertEquals(true, config.isRequired());
    Assert.assertEquals("G", config.getGroup());
    Assert.assertEquals(1, config.getDisplayPosition());
    Assert.assertEquals(2, config.getLines());
    Assert.assertEquals(3, config.getMin());
    Assert.assertEquals(4, config.getMax());
    Assert.assertEquals(ConfigDef.Evaluation.EXPLICIT, config.getEvaluation());
    Assert.assertEquals(ConfigDef.Mode.JAVA.name(), config.getMode());
    Assert.assertEquals(1, config.getElFunctionDefinitions().size());
    Assert.assertEquals(1, config.getElConstantDefinitions().size());
    Assert.assertNull(config.getModel());
    Assert.assertEquals("", config.getDependsOn());
    Assert.assertNull(config.getTriggeredByValues());
  }

  public static class Model {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @FieldSelector
    public String config;
  }

  @Test
  public void testModel() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(Model.class, "x");
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
    ConfigDefinitionExtractor.get().extract(Fail1.class, "x");
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
    ConfigDefinitionExtractor.get().extract(Fail2.class, "x");
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
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(DependsOn.class, "x");
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
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(DependsOnChain.class, "x");
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

}
