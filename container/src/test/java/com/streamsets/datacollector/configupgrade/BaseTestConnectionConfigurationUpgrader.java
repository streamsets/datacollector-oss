/*
 * Copyright 2021 StreamSets Inc.
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

package com.streamsets.datacollector.configupgrade;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ConnectionDef;
import org.junit.Assert;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class BaseTestConnectionConfigurationUpgrader {

  @Parameterized.Parameters(name = "{0}")
  public static Object[] data() {
    return TestConnectionConfigurationUpgrader.InputType.values();
  }

  // ConnectionConfigurationUpgrader has two entry points which essentially do the same thing, but one takes a
  // ConnectionDef and one takes a StageLibraryTask.  We parameterize the tests here so that we can run each test with
  // an equivalent ConnectionDef and StageLibraryTask.
  enum InputType {
    ConnectionDef,
    StageLibraryTask,
  }

  @Parameterized.Parameter
  public TestConnectionConfigurationUpgrader.InputType inputType;

  protected ConnectionDef connDef;
  protected StageLibraryTask libTask;

  protected void prep(String type, int version, String upgraderDef) {
    connDef = Mockito.mock(ConnectionDef.class);
    Mockito.when(connDef.version()).thenReturn(version);
    Mockito.when(connDef.upgraderDef()).thenReturn(upgraderDef);

    libTask = Mockito.mock(StageLibraryTask.class);
    ConnectionDefinition connectionDefinition = Mockito.mock(ConnectionDefinition.class);
    Mockito.when(connectionDefinition.getVersion()).thenReturn(version);
    Mockito.when(connectionDefinition.getUpgrader()).thenReturn(upgraderDef);
    Mockito.when(connectionDefinition.getClassLoader()).thenReturn(getClass().getClassLoader());
    Mockito.when(libTask.getConnection(type)).thenReturn(connectionDefinition);
  }

  protected List<Issue> run(ConnectionConfiguration connectionConfiguration) {
    List<Issue> issues = new ArrayList<>();
    switch (inputType) {
      case ConnectionDef:
        ConnectionConfigurationUpgrader.get().upgradeIfNecessary(connDef, connectionConfiguration, "connId", issues);
        break;
      case StageLibraryTask:
        ConnectionConfigurationUpgrader.get().upgradeIfNecessary(libTask, connectionConfiguration, issues);
        break;
      default:
        Assert.fail("Unexpected InputType: " + inputType);
    }
    return issues;
  }
}
