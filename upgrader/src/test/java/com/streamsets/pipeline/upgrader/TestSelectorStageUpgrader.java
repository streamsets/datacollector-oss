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

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestSelectorStageUpgrader {

  @Test
  public void testSelectorCallingBothUpgraders() {
    List<Config> configs = new ArrayList<>();

    StageUpgrader legacyUpgrader = Mockito.mock(StageUpgrader.class);
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-selectorUpgrader.yaml");
    SelectorStageUpgrader upgrader = new SelectorStageUpgrader("stage", legacyUpgrader, yamlResource);

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(5).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    Assert.assertEquals(3, configs.size());
    Assert.assertEquals("aaa", find(configs, "a").getValue());
    Assert.assertEquals("bbb", find(configs, "b").getValue());
    Assert.assertEquals("ccc", find(configs, "c").getValue());

    Mockito.verify(legacyUpgrader, Mockito.times(1)).upgrade(Mockito.any(), Mockito.any());
  }

  @Test
  public void testSelectorCallingYamlUpgrader() {
    List<Config> configs = new ArrayList<>();

    StageUpgrader legacyUpgrader = Mockito.mock(StageUpgrader.class);
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-selectorUpgrader.yaml");
    SelectorStageUpgrader upgrader = new SelectorStageUpgrader("stage", legacyUpgrader, yamlResource);

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(6).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    Assert.assertEquals(4, configs.size());
    Assert.assertEquals("aaa", find(configs, "a").getValue());
    Assert.assertEquals("bbb", find(configs, "b").getValue());
    Assert.assertEquals("ccc", find(configs, "c").getValue());
    Assert.assertEquals("ddd", find(configs, "d").getValue());

    Mockito.verify(legacyUpgrader, Mockito.times(0)).upgrade(Mockito.any(), Mockito.any());
  }

  @Test
  public void testSelectorCallingLegacyUpgrader() {
    List<Config> configs = new ArrayList<>();

    StageUpgrader legacyUpgrader = Mockito.mock(StageUpgrader.class);
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-selectorUpgrader.yaml");
    SelectorStageUpgrader upgrader = new SelectorStageUpgrader("stage", legacyUpgrader, yamlResource);

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    Assert.assertEquals(0, configs.size());

    Mockito.verify(legacyUpgrader, Mockito.times(1)).upgrade(Mockito.any(), Mockito.any());
  }

  @Test
  public void testSelectorCallingLegacyUpgraderAndEmptyResource() {
    List<Config> configs = new ArrayList<>();

    StageUpgrader legacyUpgrader = Mockito.mock(StageUpgrader.class);
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-empty.yaml");
    SelectorStageUpgrader upgrader = new SelectorStageUpgrader("stage", legacyUpgrader, yamlResource);

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    Assert.assertEquals(0, configs.size());

    Mockito.verify(legacyUpgrader, Mockito.times(1)).upgrade(Mockito.any(), Mockito.any());
  }

  @Test
  public void testSelectorNullLegacyUpgrader() {
    List<Config> configs = new ArrayList<>();
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-selectorUpgrader.yaml");
    SelectorStageUpgrader upgrader = new SelectorStageUpgrader("stage", null, yamlResource);

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(6).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    Assert.assertEquals(4, configs.size());
    Assert.assertEquals("aaa", find(configs, "a").getValue());
    Assert.assertEquals("bbb", find(configs, "b").getValue());
    Assert.assertEquals("ccc", find(configs, "c").getValue());
    Assert.assertEquals("ddd", find(configs, "d").getValue());
  }

  @Test
  public void testSelectorCallingOnlyYamlUpgrader() {
    List<Config> configs = new ArrayList<>();

    StageUpgrader legacyUpgrader = Mockito.mock(StageUpgrader.class);
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("test-selectorUpgrader.yaml");
    SelectorStageUpgrader upgrader = new SelectorStageUpgrader("stage", legacyUpgrader, yamlResource);

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(5).when(context).getFromVersion();
    Mockito.doReturn(6).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    Assert.assertEquals(1, configs.size());
    Assert.assertEquals("ddd", find(configs, "d").getValue());
  }

  @Test
  public void testSelectorNullYamlUpgrader() {
    List<Config> configs = new ArrayList<>();

    StageUpgrader legacyUpgrader = Mockito.mock(StageUpgrader.class);
    SelectorStageUpgrader upgrader = new SelectorStageUpgrader("stage", legacyUpgrader, null);

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    Assert.assertEquals(0, configs.size());

    Mockito.verify(legacyUpgrader, Mockito.times(1)).upgrade(Mockito.any(), Mockito.any());
  }

  private Config find(List<Config> configs, String name) {
    for (Config config : configs) {
      if (config.getName().equals(name)) {
        return config;
      }
    }
    return null;
  }
}
