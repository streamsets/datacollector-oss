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
import com.streamsets.datacollector.main.ProductBuildInfo;
import com.streamsets.datacollector.main.FileUserGroupManager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.security.usermgnt.TrxUsersManager;
import com.streamsets.datacollector.security.usermgnt.UsersManager;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.BootstrapMain;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class TestDataCollectorWebServerTask {

  @Test
  public void testGetRegistrationAttributes() throws IOException  {
    File file = new File("target", UUID.randomUUID().toString());
    try (Writer writer = new FileWriter(file)) {
    }
    UsersManager usersManager = new TrxUsersManager(file);
    BuildInfo buildInfo = ProductBuildInfo.getDefault();
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getBaseHttpUrl(true)).thenReturn("url");
    WebServerTask webServerTask = new DataCollectorWebServerTask(
        buildInfo,
        runtimeInfo,
        new Configuration(),
        new NopActivation(),
        Collections.<ContextConfigurator>emptySet(),
        Collections.<WebAppProvider>emptySet(),
        new FileUserGroupManager(usersManager),
        null
    );
    Map<String, String> expectedAttrs = ImmutableMap.<String, String>builder().put(
        SSOConstants.SERVICE_BASE_URL_ATTR,
        runtimeInfo
        .getBaseHttpUrl(true))
        .put("sdcJavaVersion", System.getProperty("java.runtime.version"))
        .put("sdcVersion", buildInfo.getVersion())
        .put("sdcBuildDate", buildInfo.getBuiltDate())
        .put("sdcRepoSha", buildInfo.getBuiltRepoSha()).build();

    webServerTask.getRegistrationAttributes();
    Assert.assertEquals(expectedAttrs, webServerTask.getRegistrationAttributes());
  }

}
