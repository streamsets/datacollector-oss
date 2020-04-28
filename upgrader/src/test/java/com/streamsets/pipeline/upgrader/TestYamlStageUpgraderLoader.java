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
package com.streamsets.pipeline.upgrader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Config;
import com.streamsets.testing.pipeline.stage.TestUpgraderContext;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestYamlStageUpgraderLoader {

  private Config find(List<Config> configs, String name) {
    for (Config config : configs) {
      if (config.getName().equals(name)) {
        return config;
      }
    }
    return null;
  }

  @Test
  public void testToVersion() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-yamlUpgraderToVersion.yaml");
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    YamlStageUpgrader upgrader = loader.get();

    List<Config> configs = upgrader.upgrade(new ArrayList<>(), new TestUpgraderContext("lib", "stage", "instance", 0, 0));
    Assert.assertEquals(0, configs.size());

    configs = upgrader.upgrade(new ArrayList<>(), new TestUpgraderContext("lib", "stage", "instance", 0, 1));
    Assert.assertEquals(1, configs.size());
    Assert.assertNotNull(find(configs, "u1"));

    configs = upgrader.upgrade(new ArrayList<>(), new TestUpgraderContext("lib", "stage", "instance", 1, 2));
    Assert.assertEquals(1, configs.size());
    Assert.assertNotNull(find(configs, "u2"));

    configs = upgrader.upgrade(new ArrayList<>(), new TestUpgraderContext("lib", "stage", "instance", 0, 3));
    Assert.assertEquals(3, configs.size());
    Assert.assertNotNull(find(configs, "u1"));
    Assert.assertNotNull(find(configs, "u2"));
    Assert.assertNotNull(find(configs, "u3"));
  }

  @Test
  public void testSetConfigAction() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-yamlUpgraderActions.yaml");
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    YamlStageUpgrader upgrader = loader.get();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("set2", false));
    configs.add(new Config("listConfig", ImmutableList.of(ImmutableMap.of())));
    configs = upgrader.upgrade(configs, new TestUpgraderContext("lib", "stage", "instance", 0, 1));
    Assert.assertEquals(5, configs.size());

    //testing EL
    Assert.assertEquals("false/", find(configs, "set1").getValue());
    Assert.assertEquals(true, find(configs, "set2").getValue());
    Assert.assertEquals(1, find(configs, "set3").getValue());
    Assert.assertEquals(Collections.emptyList(), find(configs, "set4").getValue());
    Assert.assertEquals(ImmutableList.of(ImmutableMap.of("setI", "SET")), find(configs, "listConfig").getValue());
  }

  @Test
  public void testSetConfigActionWithLookForName() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-yamlUpgraderActions.yaml");
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    YamlStageUpgrader upgrader = loader.get();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("a", "A"));
    configs.add(new Config("c", "xFOOx"));
    configs.add(new Config("d", ImmutableList.of(1)));
    configs.add(new Config("listConfig", ImmutableList.of(ImmutableMap.of("b", "BB"))));
    configs = upgrader.upgrade(configs, new TestUpgraderContext("lib", "stage", "instance", 1, 2));
    Assert.assertEquals(8, configs.size());
    Assert.assertEquals("A", find(configs, "a").getValue());
    Assert.assertEquals("X", find(configs, "x").getValue());
    Assert.assertEquals("xFOOx", find(configs, "c").getValue());
    Assert.assertEquals(true, find(configs, "new1").getValue());
    Assert.assertEquals(ImmutableList.of(1), find(configs, "d").getValue());
    Assert.assertEquals(ImmutableList.of(), find(configs, "new2").getValue());
    Assert.assertEquals("Z", find(configs, "z").getValue());
    Assert.assertEquals(ImmutableList.of(ImmutableMap.of("b", "B")), find(configs, "listConfig").getValue());
  }

  @Test
  public void testSetConfigActionWithElse() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-yamlUpgraderActions.yaml");
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    YamlStageUpgrader upgrader = loader.get();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("a", "A"));
    configs.add(new Config("c", "BAR"));
    configs.add(new Config("d", "xFOOx"));
    configs.add(new Config("e", "BAR"));
    configs.add(new Config("listConfig", ImmutableList.of(ImmutableMap.of("b", "B"))));
    configs = upgrader.upgrade(configs, new TestUpgraderContext("lib", "stage", "instance", 2, 3));
    Assert.assertEquals(10, configs.size());
    Assert.assertEquals("A", find(configs, "a").getValue());
    Assert.assertEquals("Y", find(configs, "y").getValue());
    Assert.assertEquals("BAR", find(configs, "c").getValue());
    Assert.assertEquals(true, find(configs, "new1").getValue());
    Assert.assertEquals("xFOOx", find(configs, "d").getValue());
    Assert.assertEquals(true, find(configs, "caseA").getValue());
    Assert.assertEquals("BAR", find(configs, "e").getValue());
    Assert.assertEquals(5, find(configs, "caseD").getValue());
    Assert.assertEquals("Z", find(configs, "z").getValue());
    Assert.assertEquals(ImmutableList.of(ImmutableMap.of("a", "A", "b", "B")), find(configs, "listConfig").getValue());
  }

  @Test
  public void testSetConfigActionWithNewConfigsAndELs() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-yamlUpgraderActions.yaml");
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    YamlStageUpgrader upgrader = loader.get();

    List<Config> configs = new ArrayList<>();
    configs = upgrader.upgrade(configs, new TestUpgraderContext("lib", "stage", "instance", 9, 10));
    Assert.assertEquals(4, configs.size());
    Assert.assertEquals("newValue", find(configs, "newConfig").getValue());
    Assert.assertEquals("newValue", find(configs, "newConfigUsingOtherNewConfig").getValue());
    Assert.assertEquals("newValue", find(configs, "newIfConfig1").getValue());
    Assert.assertEquals("newValue", find(configs, "newIfConfig1").getValue());
  }

  @Test
  public void testRenameConfigAction() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-yamlUpgraderActions.yaml");
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    YamlStageUpgrader upgrader = loader.get();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("x", "X"));
    configs.add(new Config("old", "V1"));
    configs.add(new Config("old.a", "V2"));
    configs.add(new Config("listConfig", ImmutableList.of(ImmutableMap.of("old", "V3"))));
    configs = upgrader.upgrade(configs, new TestUpgraderContext("lib", "stage", "instance", 3, 4));
    Assert.assertEquals(4, configs.size());
    Assert.assertEquals("X", find(configs, "x").getValue());
    Assert.assertEquals("V1", find(configs, "new").getValue());
    Assert.assertEquals("V2", find(configs, "new.a").getValue());
    Assert.assertEquals(ImmutableList.of(ImmutableMap.of("new", "V3")), find(configs, "listConfig").getValue());
  }

  @Test
  public void testRemoveConfigs() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-yamlUpgraderActions.yaml");
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    YamlStageUpgrader upgrader = loader.get();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("a", "A"));
    configs.add(new Config("b", "B"));
    configs.add(new Config("a.x", "AX"));
    configs.add(new Config("listConfig", ImmutableList.of(ImmutableMap.of("a", "A", "b", "B"))));
    configs = upgrader.upgrade(configs, new TestUpgraderContext("lib", "stage", "instance", 4, 5));
    Assert.assertEquals(2, configs.size());
    Assert.assertEquals("B", find(configs, "b").getValue());
    Assert.assertEquals(ImmutableList.of(ImmutableMap.of("a", "A")), find(configs, "listConfig").getValue());
  }

  @Test
  public void testReplaceConfigs() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-yamlUpgraderActions.yaml");
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    YamlStageUpgrader upgrader = loader.get();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("x", "X"));
    configs.add(new Config("a", "A"));
    configs.add(new Config("aa", "A"));
    configs.add(new Config("aaa", "B"));
    configs.add(new Config("aaaa", true));
    configs.add(new Config("aaaaa", false));
    configs.add(new Config("b", "xFOOx"));
    configs.add(new Config("bb", "X"));
    configs.add(new Config("bbb", "foo"));
    configs.add(new Config("bbbb", "bar"));
    configs.add(new Config("listConfig", ImmutableList.of(ImmutableMap.of("a", "A"))));
    configs = upgrader.upgrade(configs, new TestUpgraderContext("lib", "stage", "instance", 5, 6));
    Assert.assertEquals(11, configs.size());
    Assert.assertEquals("X", find(configs, "x").getValue());
    // testing EL
    Assert.assertEquals("X/AA", find(configs, "a").getValue());
    Assert.assertEquals("AA", find(configs, "aa").getValue());
    Assert.assertEquals("B", find(configs, "aaa").getValue());
    Assert.assertEquals("C", find(configs, "aaaa").getValue());
    // testing EL
    Assert.assertEquals("X/CC", find(configs, "aaaaa").getValue());
    Assert.assertEquals("BAR", find(configs, "b").getValue());
    Assert.assertEquals("NOFOO", find(configs, "bb").getValue());
    Assert.assertEquals("oldvalue=foo", find(configs, "bbb").getValue());
    Assert.assertEquals("oldvalue=bar", find(configs, "bbbb").getValue());
    Assert.assertEquals(ImmutableList.of(ImmutableMap.of("a", "AA")), find(configs, "listConfig").getValue());
  }

  @Test
  public void testStringCollectionsConfigs() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-yamlUpgraderActions.yaml");
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    YamlStageUpgrader upgrader = loader.get();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("map1", null));
    configs.add(new Config(
        "map2",
        ImmutableList.of(ImmutableMap.of("key", "key", "value", "value"), ImmutableMap.of("key", "foo", "value", "bar"))
    ));
    configs.add(new Config(
        "map3",
        ImmutableList.of(ImmutableMap.of("key", "key", "value", "value"), ImmutableMap.of("key", "foo", "value", "bar"))
    ));
    configs.add(new Config("list1", null));
    configs.add(new Config("list2", ImmutableList.of("value", "foo")));
    configs.add(new Config("listConfig", ImmutableList.of(ImmutableMap.of(
        "map1",
        ImmutableList.of(),
        "map2",
        ImmutableList.of(
            ImmutableMap.of("key", "key", "value", "value"),
            ImmutableMap.of("key", "foo", "value", "bar")
        ),
        "map3",
        ImmutableList.of(
            ImmutableMap.of("key", "key", "value", "value"),
            ImmutableMap.of("key", "foo", "value", "bar")
        ),
        "list1",
        ImmutableList.of(),
        "list2",
        ImmutableList.of("value", "foo")
    ))));
    configs = upgrader.upgrade(configs, new TestUpgraderContext("lib", "stage", "instance", 6, 7));
    Assert.assertEquals(7, configs.size());
    Assert.assertEquals(
        ImmutableList.of(ImmutableMap.of("key", "key", "value", "value")),
        find(configs, "map1").getValue()
    );
    Assert.assertEquals(
        ImmutableList.of(ImmutableMap.of("key", "foo", "value", "bar")),
        find(configs, "map2").getValue()
    );
    Assert.assertEquals(
        ImmutableList.of(ImmutableMap.of("key", "foo", "value", "bar")),
        find(configs, "map3").getValue()
    );
    Assert.assertEquals(ImmutableList.of("value"), find(configs, "list1").getValue());
    Assert.assertEquals(ImmutableList.of("foo"), find(configs, "list2").getValue());
    Assert.assertEquals(ImmutableList.of(ImmutableMap.of("key", "key", "value", "value")), (
        (Map) ((List)find(configs, "listConfig").getValue()).get(0)).get("map1"));
    Assert.assertEquals(ImmutableList.of(ImmutableMap.of("key", "foo", "value", "bar")), (
        (Map) ((List)find(configs, "listConfig").getValue()).get(0)).get("map2"));
    Assert.assertEquals(ImmutableList.of(ImmutableMap.of("key", "foo", "value", "bar")), (
        (Map) ((List)find(configs, "listConfig").getValue()).get(0)).get("map3"));
    Assert.assertEquals(ImmutableList.of("value"), ((Map) ((List) find(configs, "listConfig").getValue()).get(0)).get(
        "list1"));
    Assert.assertEquals(ImmutableList.of("foo"), ((Map) ((List) find(configs, "listConfig").getValue()).get(0)).get(
        "list2"));

    configs = upgrader.upgrade(configs, new TestUpgraderContext("lib", "stage", "instance", 7, 8));

    Assert.assertEquals(ImmutableList.of("valueFromOldVariable"), find(configs, "list3").getValue());

  }

  public class Service1 {
  }

  public class Service2 {
  }

  public class Service3 {
  }

  @Test
  public void testRegisterServiceConfigs() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-yamlUpgraderActions.yaml");
    YamlStageUpgraderLoader loader = new YamlStageUpgraderLoader("stage", yamlResource);
    YamlStageUpgrader upgrader = loader.get();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config(
        "bean1.innerBean.prop1",
        "prop1Value"
    ));
    configs.add(new Config(
        "bean2.innerBean1.prop1",
        "prop1Value"
    ));
    configs.add(new Config(
        "bean2.innerBean1.prop2",
        "prop2Value"
    ));
    List innerListValues = ImmutableList.of(ImmutableMap.of("key", "key", "value", "value"), ImmutableMap.of("key", "foo", "value", "bar"));
    configs.add(new Config("bean2.innerBean2.innerList", innerListValues));

    Map innerMapValues = ImmutableMap.of("key", "key", "value", "value");
    configs.add(new Config("bean2.innerBean2.innerMap", innerMapValues));

    TestUpgraderContext upgradeContext = new TestUpgraderContext("lib", "stage", "instance", 8, 9);
    configs = upgrader.upgrade(configs, upgradeContext);
    Assert.assertEquals(1, configs.size());
    Assert.assertEquals("bean1.innerBean.prop1", configs.get(0).getName());
    Assert.assertEquals("prop1Value", configs.get(0).getValue());

    Assert.assertEquals(3, upgradeContext.getRegisteredServices().size());

    Assert.assertNotNull(upgradeContext.getRegisteredServices().get(Service1.class));
    List<Config> service1Config = upgradeContext.getRegisteredServices().get(Service1.class);

    Assert.assertEquals(2, service1Config.size());
    Assert.assertTrue(service1Config.stream().anyMatch(s -> s.getName().equals("prop1")));
    Assert.assertTrue(service1Config.stream().anyMatch(s -> s.getName().equals("prop2")));

    Assert.assertEquals("prop1Value", service1Config.stream().filter(s -> s.getName().equals("prop1")).findFirst().get().getValue());
    Assert.assertEquals("prop2Value", service1Config.stream().filter(s -> s.getName().equals("prop2")).findFirst().get().getValue());

    Assert.assertNotNull(upgradeContext.getRegisteredServices().get(Service2.class));
    List<Config> service2Config = upgradeContext.getRegisteredServices().get(Service2.class);

    Assert.assertEquals(2, service2Config.size());
    Assert.assertTrue(service2Config.stream().anyMatch(s -> s.getName().equals("innerList")));
    Assert.assertTrue(service2Config.stream().anyMatch(s -> s.getName().equals("innerMap")));

    Assert.assertEquals(innerListValues, service2Config.stream().filter(s -> s.getName().equals("innerList")).findFirst().get().getValue());
    Assert.assertEquals(innerMapValues, service2Config.stream().filter(s -> s.getName().equals("innerMap")).findFirst().get().getValue());

    Assert.assertNotNull(upgradeContext.getRegisteredServices().get(Service3.class));
    Assert.assertEquals(0, upgradeContext.getRegisteredServices().get(Service3.class).size());
  }
}
