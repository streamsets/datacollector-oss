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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.dirspooler.FileOrdering;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestSpoolDirSourceUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/SpoolDirDSource.yaml");
    upgrader = new SelectorStageUpgrader("stage", new SpoolDirSourceUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testSpoolDirSourceUpgrader() throws StageException {
    SpoolDirSourceUpgrader spoolDirSourceUpgrader = new SpoolDirSourceUpgrader();

    List<Config> upgrade = spoolDirSourceUpgrader.upgrade("x", "y", "z", 1, 7, new ArrayList<Config>());
    assertEquals(9, upgrade.size());
    assertEquals("conf.dataFormatConfig.compression", upgrade.get(0).getName());
    assertEquals("NONE", upgrade.get(0).getValue());
    assertEquals("conf.dataFormatConfig.csvCustomDelimiter", upgrade.get(1).getName());
    assertEquals('|', upgrade.get(1).getValue());
    assertEquals("conf.dataFormatConfig.csvCustomEscape", upgrade.get(2).getName());
    assertEquals('\\', upgrade.get(2).getValue());
    assertEquals("conf.dataFormatConfig.csvCustomQuote", upgrade.get(3).getName());
    assertEquals('\"', upgrade.get(3).getValue());
    assertEquals("conf.dataFormatConfig.csvRecordType", upgrade.get(4).getName());
    assertEquals("LIST", upgrade.get(4).getValue());
    assertEquals("conf.dataFormatConfig.filePatternInArchive", upgrade.get(5).getName());
    assertEquals("*", upgrade.get(5).getValue());
    assertEquals("conf.dataFormatConfig.csvSkipStartLines", upgrade.get(6).getName());
    assertEquals(0, upgrade.get(6).getValue());
    assertEquals("conf.allowLateDirectory", upgrade.get(7).getName());
    assertEquals(false, upgrade.get(7).getValue());
    assertEquals("conf.useLastModified", upgrade.get(8).getName());
    assertEquals(FileOrdering.LEXICOGRAPHICAL.name(), upgrade.get(8).getValue());
  }

  @Test
  public void testV8toV9() throws StageException {
    SpoolDirSourceUpgrader spoolDirSourceUpgrader = new SpoolDirSourceUpgrader();

    List<Config> configs = new ArrayList<>();
    List<Config> upgraded = spoolDirSourceUpgrader.upgrade("x", "y", "z", 8, 9, configs);

    assertEquals(1, upgraded.size());
    assertEquals(PathMatcherMode.GLOB, upgraded.get(0).getValue());
    assertEquals("conf.pathMatcherMode", upgraded.get(0).getName());
  }

  @Test
  public void testV9toV10() throws StageException {
    SpoolDirSourceUpgrader spoolDirSourceUpgrader = new SpoolDirSourceUpgrader();

    List<Config> configs = new ArrayList<>();
    List<Config> upgraded = spoolDirSourceUpgrader.upgrade("x", "y", "z", 9, 10, configs);

    assertEquals(1, upgraded.size());
    assertEquals(5, upgraded.get(0).getValue());
    assertEquals("conf.spoolingPeriod", upgraded.get(0).getName());
  }

  @Test
  public void testV10ToV11() {
    Mockito.doReturn(10).when(context).getFromVersion();
    Mockito.doReturn(11).when(context).getToVersion();

    String dataFormatPrefix = "conf.dataFormatConfig.";
    configs.add(new Config(dataFormatPrefix + "preserveRootElement", true));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "preserveRootElement", false);
  }
}
