/**
 * Copyright 2016 StreamSets Inc.
 */
package com.streamsets.datacollector.http;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.DataCollectorBuildInfo;
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
    WebServerTask webServerTask = new DataCollectorWebServerTask(buildInfo, runtimeInfo, new Configuration(),
        Collections.<ContextConfigurator>emptySet(),
        Collections.<WebAppProvider>emptySet());
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
