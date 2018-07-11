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
package com.streamsets.datacollector.main;

import com.streamsets.datacollector.util.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class RuntimeInfoTest {

  public RuntimeInfo generateRuntimeInfo() throws IOException {
    Path dataDir = Files.createTempDirectory("data-dir");
    Path confDir = Files.createTempDirectory("conf-dir");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(dataDir.toString());
    Mockito.when(runtimeInfo.getConfigDir()).thenReturn(confDir.toString());

    return runtimeInfo;
  }

  @Test
  public void testLoadOrReloadConfigs() throws IOException {
    RuntimeInfo runtimeInfo = generateRuntimeInfo();

    // Generate both standard sdc.properties as well as control hub configuration override
    Files.write(
      Paths.get(runtimeInfo.getConfigDir(), "sdc.properties"),
      Collections.singletonList("sdc=true")
    );
    Files.write(
      Paths.get(runtimeInfo.getDataDir(), RuntimeInfo.SCH_CONF_OVERRIDE),
      Collections.singletonList("sch=true")
    );

    // Validate that we see expected configs
    Configuration configuration = new Configuration();
    RuntimeInfo.loadOrReloadConfigs(runtimeInfo, configuration);
    assertEquals("true", configuration.get("sdc", null));
    assertEquals("true", configuration.get("sch", null));
  }

  @Test
  public void testStoreControlHubConfigs() throws IOException {
    RuntimeInfo runtimeInfo = generateRuntimeInfo();

    // Save new config
    RuntimeInfo.storeControlHubConfigs(runtimeInfo, Collections.singletonMap("sch", "true"));

    // And validate that it's properly loaded back
    Configuration configuration = new Configuration();
    RuntimeInfo.loadOrReloadConfigs(runtimeInfo, configuration);
    assertEquals("true", configuration.get("sch", null));
  }
}
