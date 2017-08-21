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
package com.streamsets.datacollector.http;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.activation.NopActivation;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.DataCollectorBuildInfo;
import com.streamsets.datacollector.main.FileUserGroupManager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.SSOConstants;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class TestDataCollectorWebServerTask {

  @Test
  public void testGetRegistrationAttributes() {
    BuildInfo buildInfo = new DataCollectorBuildInfo();
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getBaseHttpUrl()).thenReturn("url");
    WebServerTask webServerTask = new DataCollectorWebServerTask(
        buildInfo,
        runtimeInfo,
        new Configuration(),
        new NopActivation(),
        Collections.<ContextConfigurator>emptySet(),
        Collections.<WebAppProvider>emptySet(),
        new FileUserGroupManager()
    );
    Map<String, String> expectedAttrs = ImmutableMap.<String, String>builder().put(SSOConstants.SERVICE_BASE_URL_ATTR,
        runtimeInfo
        .getBaseHttpUrl())
        .put("sdcJavaVersion", System.getProperty("java.runtime.version"))
        .put("sdcVersion", buildInfo.getVersion())
        .put("sdcBuildDate", buildInfo.getBuiltDate())
        .put("sdcRepoSha", buildInfo.getBuiltRepoSha()).build();

    webServerTask.getRegistrationAttributes();
    Assert.assertEquals(expectedAttrs, webServerTask.getRegistrationAttributes());
  }

}
