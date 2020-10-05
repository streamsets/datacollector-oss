/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.definition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.PredicateModel;

import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
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

  public static class ImplicitOnlyEls {
    @ElFunction(prefix = "p", name = "fi", description = "ff", implicitOnly = true)
    public static String fi() {
      return null;
    }
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
        elDefs = {ELs.class }
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
        elDefs = ELs.class,
        displayMode = ConfigDef.DisplayMode.ADVANCED
    )
    public String config;
  }

  public static class Ok3 {

    @ConfigDef(
        label = "L",
        description = "D",
        type = ConfigDef.Type.MODEL,
        required = true,
        elDefs = {ELs.class, ImplicitOnlyEls.class }
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
    Assert.assertEquals(ConfigDef.DisplayMode.BASIC, config.getDisplayMode());
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

    Assert.assertEquals(ConfigDef.DisplayMode.ADVANCED, config.getDisplayMode());
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

  public static class ListBeanDefaultEmptyString {

    @ConfigDef(
        label = "L",
        description = "D",
        type = ConfigDef.Type.MODEL,
        defaultValue = "",
        required = true,
        elDefs = ELs.class
    )
    @ListBeanModel
    public List<Ok4> inner;
  }
  @Test
  public void testDefaultExtractionOfListBeanModelWithEmptyString() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(ListBeanDefaultEmptyString.class, Collections.emptyList(), "x");
    Assert.assertEquals(1, configs.size());
    ConfigDefinition config = configs.get(0);

    Assert.assertEquals(ConfigDef.Type.MODEL, config.getType());
    Assert.assertEquals(null, config.getDefaultValue());
  }

  public static class ListBeanDefaultEmptyArray {

    @ConfigDef(
        label = "L",
        description = "D",
        type = ConfigDef.Type.MODEL,
        defaultValue = "[]",
        required = true,
        elDefs = ELs.class
    )
    @ListBeanModel
    public List<Ok4> inner;
  }
  @Test
  public void testDefaultExtractionOfListBeanModelWithEmptyArray() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(ListBeanDefaultEmptyArray.class, Collections.emptyList(), "x");
    Assert.assertEquals(1, configs.size());
    ConfigDefinition config = configs.get(0);

    Assert.assertEquals(ConfigDef.Type.MODEL, config.getType());
    Assert.assertEquals(Collections.emptyList(), config.getDefaultValue());
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

  public static class Fail4 {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = false,
        min = 0,
        evaluation = ConfigDef.Evaluation.EXPLICIT,
        elDefs = ImplicitOnlyEls.class
    )
    public static String config;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStringConfigFail4() {
    ConfigDefinitionExtractor.get().extract(Fail4.class, Collections.<String>emptyList(), "x");
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
    Assert.assertEquals(1, a.getDependsOnMap().size());
    Assert.assertEquals(ImmutableSet.of("B"), new HashSet<>(a.getDependsOnMap().get("b")));
    Assert.assertEquals("", b.getDependsOn());
    Assert.assertNull(b.getTriggeredByValues());
  }

  public static class MultipleDependsOn {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependencies = {
            @Dependency(configName = "b", triggeredByValues = {"B", "C"}),
            @Dependency(configName = "e", triggeredByValues = {"E", "F"})
        }
    )
    public String a;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true
    )
    public String b;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true
    )
    public String e;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependsOn = "a",
        triggeredByValue = "A"
    )
    public String f;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependencies = {
            @Dependency(configName = "a", triggeredByValues = {"A"}),
            @Dependency(configName = "e", triggeredByValues = {"G"}),
            @Dependency(configName = "f", triggeredByValues = {"F", "FF"})
        }
    )
    public String g;
  }

  @Test
  public void testMultipleDependsOn() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(MultipleDependsOn.class,
        Collections.<String>emptyList(),"x");
    Assert.assertEquals(5, configs.size());
    ConfigDefinition a = null;
    ConfigDefinition b = null;
    ConfigDefinition e = null;
    ConfigDefinition f = null;
    ConfigDefinition g = null;

    for (ConfigDefinition config : configs) {
        switch (config.getName()) {
          case ("a"):
            a = config;
            break;
          case ("b"):
            b = config;
            break;
          case ("e"):
            e = config;
            break;
          case ("f"):
            f = config;
            break;
          default:
            g = config;
        }
    }
    Assert.assertEquals(2, a.getDependsOnMap().size());
    Assert.assertEquals(ImmutableSet.of("B", "C"), new HashSet<>(a.getDependsOnMap().get("b")));
    Assert.assertEquals(ImmutableSet.of("E", "F"), new HashSet<>(a.getDependsOnMap().get("e")));
    Assert.assertEquals(3, f.getDependsOnMap().size());
    Assert.assertEquals(ImmutableSet.of("A"), new HashSet<>(f.getDependsOnMap().get("a")));
    Assert.assertEquals(ImmutableSet.of("B", "C"), new HashSet<>(f.getDependsOnMap().get("b")));
    Assert.assertEquals(ImmutableSet.of("E", "F"), new HashSet<>(f.getDependsOnMap().get("e")));
    Assert.assertEquals(4, g.getDependsOnMap().size());
    Assert.assertEquals(ImmutableSet.of("A"), new HashSet<>(g.getDependsOnMap().get("a")));
    Assert.assertEquals(ImmutableSet.of("B", "C"), new HashSet<>(g.getDependsOnMap().get("b")));
    Assert.assertEquals(ImmutableSet.of("F", "FF"), new HashSet<>(g.getDependsOnMap().get("f")));
    Assert.assertTrue(b.getDependsOnMap().isEmpty());
    Assert.assertTrue(e.getDependsOnMap().isEmpty());
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
    Assert.assertEquals(2, a.getDependsOnMap().size());
    Assert.assertEquals(ImmutableSet.of("B"), new HashSet<>(a.getDependsOnMap().get("b")));
    Assert.assertEquals(ImmutableSet.of("C"), new HashSet<>(a.getDependsOnMap().get("c")));
  }

  public static class CycleDependencies {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependencies = {
            @Dependency(configName = "c", triggeredByValues = {"anything"}),
        }
    )
    public String a;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependsOn = "t",
        triggeredByValue = "f"
    )
    public String c;


    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependsOn = "f",
        triggeredByValue = "o"
    )
    public String b;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependencies = {
            @Dependency(configName = "g", triggeredByValues = "random")
        },
        dependsOn = "b",
        triggeredByValue = "really don't care"
    )
    public String e;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependencies = {
            @Dependency(configName = "b", triggeredByValues = "random"),
            @Dependency(configName = "e", triggeredByValues = "dds"),
            @Dependency(configName = "f", triggeredByValues = "f"),
            @Dependency(configName = "g", triggeredByValues = "g")
        }
    )
    public String f;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependencies = {
            @Dependency(configName = "a", triggeredByValues = {"A"}),
            @Dependency(configName = "c", triggeredByValues = {"A"}),
        }
    )
    public String g;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true,
        dependsOn = "g",
        triggeredByValue = "y"
    )
    public String t;
  }

  @Test
  public void testDependsOnCycle() {
    try {
      ConfigDefinitionExtractor.get().extract(CycleDependencies.class, Collections.<String>emptyList(), "x");
      Assert.fail();
    } catch (IllegalStateException ex) {
      Set<String> cycles = ConfigDefinitionExtractor.get().getCycles();
      Assert.assertEquals(5, cycles.size());
      Assert.assertTrue(cycles.contains("a -> c -> t -> g -> a"));
      Assert.assertTrue(cycles.contains("b -> f -> e -> b"));
      Assert.assertTrue(cycles.contains("c -> t -> g -> c"));
      Assert.assertTrue(cycles.contains("b -> f -> b"));
      Assert.assertTrue(cycles.contains("f -> f"));
    }
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
    List<ErrorMessage> errors = ConfigDefinitionExtractor.get().validate(Bean.class, ImmutableList.of("bar", "foo"),
        "x");
    Assert.assertTrue(errors.isEmpty());
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

  public static class BeanA {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING
    )
    public String prop;

  }

  public static class BeanASubClass extends BeanA {

    @ConfigDef(
        label = "LShadow",
        required = false,
        type = ConfigDef.Type.STRING
    )
    public String prop;
  }

  @Test
  public void testConfigBeanSubclassPropertyShadowing() {
    List<ErrorMessage> errors = ConfigDefinitionExtractor.get().validate(BeanASubClass.class, ImmutableList.of(), "x");
    Assert.assertTrue(errors.isEmpty());
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(
        BeanASubClass.class,
        ImmutableList.of(),
        "x"
    );
    Assert.assertEquals(1, configs.size());
    Assert.assertEquals("prop", configs.get(0).getFieldName());
    Assert.assertEquals("LShadow", configs.get(0).getLabel());
    Assert.assertEquals(false, configs.get(0).isRequired());
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
    List<ErrorMessage> errorMessages = ConfigDefinitionExtractor.get().validate(Bean2.class, Collections.<String>emptyList(),
        "x");
    Assert.assertEquals(3, errorMessages.size());
  }


  public static class SubBean3 {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING,
        dependsOn = "props^"
    )
    public String prop3;

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING,
        dependencies = {
            @Dependency(configName = "props3", triggeredByValues = {"someval1"}),
            @Dependency(configName = "props6", triggeredByValues = {"someval2"})
        }
    )
    public String prop6;

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING,
        dependencies = {
            @Dependency(configName = "propInt^", triggeredByValues = {"testing"})
        }
    )
    public String prop7;

  }

  public static class Bean3 {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING
    )
    public String prop1;

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.NUMBER
    )
    public int propInt;

    @ConfigDefBean
    public SubBean3 prop2;
  }

  @Test
  public void testConfigDefBeanNonExistentDependsOn() {
    List<ErrorMessage> errorMessages =
        ConfigDefinitionExtractor.get().validate(Bean3.class, Collections.<String>emptyList(), "x");
    Assert.assertEquals(4, errorMessages.size());
  }

  public static class Bean4 {

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.CREDENTIAL,
        upload = ConfigDef.Upload.BASE64
    )
    public CredentialValue credential;

  }

  @Test
  public void testCredentialValue() {
    List<ConfigDefinition> defs = ConfigDefinitionExtractor.get().extract(Bean4.class, Collections.<String>emptyList(), "x");
    Assert.assertEquals(1, defs.size());
    ConfigDefinition def = defs.get(0);
    Assert.assertEquals(ConfigDef.Upload.BASE64, def.getUpload());
    List<ElFunctionDefinition> fDefs = def.getElFunctionDefinitions();
    Set<String> fNames = ImmutableSet.copyOf(Lists.transform(fDefs, input -> input.getName()));
    Assert.assertTrue(fNames.contains("credential:get"));
    Assert.assertTrue(fNames.contains("credential:getWithOptions"));
  }

  public static class DefaultFromResourceClass {
    @ConfigDef(
        defaultValueFromResource = "test-default-value-from-resources.txt",
        type = ConfigDef.Type.STRING,
        required = true,
        label = "L"
    )
    public String stringConfig;
  }

  @Test
  public void testDefaultValueFromResources() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(DefaultFromResourceClass.class,
        Collections.<String>emptyList(), "x");
    Assert.assertEquals(1, configs.size());
    ConfigDefinition config = configs.get(0);
    Assert.assertEquals("This was read from test-default-value-from-resources.txt", config.getDefaultValue());
  }

  public static class InvalidDefaultFromResourceClass {
    @ConfigDef(
        defaultValueFromResource = "not-a-real-filepath.whatever",
        type = ConfigDef.Type.STRING,
        required = true,
        label = "L"
    )
    public String stringConfig;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDefaultValueFromResources() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(InvalidDefaultFromResourceClass.class,
        Collections.<String>emptyList(), "x");
  }

  public static class BothDefaultAndDefaultFromResourceClass {
    @ConfigDef(
        defaultValue = "thisDefault",
        defaultValueFromResource = "test-default-value-from-resources.txt",
        type = ConfigDef.Type.STRING,
        required = true,
        label = "L"
    )
    public String stringConfig;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBothDefaultAndDefaultValueFromResources() {
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(
        BothDefaultAndDefaultFromResourceClass.class,
        Collections.<String>emptyList(),
        "x"
    );
  }

  public static class SubSubBean5 {
    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING
    )
    public String prop4;

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING,
        dependsOn = "prop4",
        triggeredByValue = "abc"
    )
    public String prop5;

    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING,
        dependencies = {
            @Dependency(configName = "prop4", triggeredByValues = {"xyz"})
        }
    )
    public String prop6;
  }

  public static class SubBean5 {
    @ConfigDefBean()
    public SubSubBean5 prop3;
  }

  public static class Bean5 {
    @ConfigDef(
        label = "L",
        required = true,
        type = ConfigDef.Type.STRING
    )
    public String prop1;

    @ConfigDefBean(
        dependencies = {
            @Dependency(configName = "prop1", triggeredByValues = {"trigger"})
        })
    public SubBean5 prop2;
  }

  @Test
  public void testMultipleLevelsConfigDefBeanDependency() {
    List<ErrorMessage> errors = ConfigDefinitionExtractor.get().validate(Bean5.class, ImmutableList.of("bar", "foo"),
            "x");
    Assert.assertTrue(errors.isEmpty());
    List<ConfigDefinition> configs = ConfigDefinitionExtractor.get().extract(Bean5.class,
            ImmutableList.of("bar", "foo"), "x");
    Assert.assertEquals(4, configs.size());
    configs.sort(Comparator.comparing(ConfigDefinition::getName)); // Ensure consistent ordering for retrieval

    Assert.assertEquals("prop1", configs.get(0).getName());
    Assert.assertEquals(0, configs.get(0).getDependsOnMap().size());

    Assert.assertEquals("prop2.prop3.prop4", configs.get(1).getName());
    Map<String, List<Object>> dependsOnMap1 = configs.get(1).getDependsOnMap();
    Assert.assertEquals(1, dependsOnMap1.size());
    Assert.assertEquals(Lists.newArrayList("trigger"), dependsOnMap1.get("prop1"));

    Assert.assertEquals("prop2.prop3.prop5", configs.get(2).getName());
    Map<String, List<Object>> dependsOnMap2 = configs.get(2).getDependsOnMap();
    Assert.assertEquals(2, dependsOnMap2.size());
    Assert.assertEquals(Lists.newArrayList("trigger"), dependsOnMap2.get("prop1"));
    Assert.assertEquals(Lists.newArrayList("abc"), dependsOnMap2.get("prop2.prop3.prop4"));

    Assert.assertEquals("prop2.prop3.prop6", configs.get(3).getName());
    Map<String, List<Object>> dependsOnMap3 = configs.get(3).getDependsOnMap();
    Assert.assertEquals(2, dependsOnMap3.size());
    Assert.assertEquals(Lists.newArrayList("trigger"), dependsOnMap3.get("prop1"));
    Assert.assertEquals(Lists.newArrayList("xyz"), dependsOnMap3.get("prop2.prop3.prop4"));
  }

  public static class Bean6 {

    @ConfigDef(
      label = "L",
      required = true,
      type = ConfigDef.Type.STRING,
      connectionType = "FOO"
    )
    public String connectionSelection;

  }

  @Test
  public void testConnectionType() {
    List<ConfigDefinition> defs = ConfigDefinitionExtractor.get().extract(Bean6.class, Collections.emptyList(), "x");
    Assert.assertEquals(1, defs.size());
    ConfigDefinition def = defs.get(0);
    Assert.assertEquals("FOO", def.getConnectionType());
  }
}
