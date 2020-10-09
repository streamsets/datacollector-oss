/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.lib.security.http.aster;

import com.streamsets.datacollector.http.AsterConfig;
import com.streamsets.datacollector.http.AsterContext;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAsterContextCreator {

  @Test
  public void testCreateDisabled() {
    AsterContextCreator creator = new AsterContextCreator();
    Configuration engineConf = new Configuration();
    engineConf.set(AsterServiceProvider.ASTER_URL, "");
    AsterConfig config = new AsterConfig(
        "DC",
        "version",
        "id",
        engineConf,
        "/tmp"
    );
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    AsterContext context = creator.apply(runtimeInfo, config);

    Assert.assertNotNull(context);
    Assert.assertFalse(context.isEnabled());
  }

  @Test
  public void testCreateEnabled() {
    AsterContextCreator creator = new AsterContextCreator();
    Configuration engineConf = new Configuration();
    engineConf.set(AsterServiceProvider.ASTER_URL, "https://dummy-aster");
    AsterConfig config = new AsterConfig(
        "DC",
        "version",
        "id",
        engineConf,
        "/tmp"
    );
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    AsterContext context = creator.apply(runtimeInfo, config);

    Assert.assertNotNull(context);
    Assert.assertTrue(context.isEnabled());
    Assert.assertNotNull(context.getService());
    Assert.assertNotNull(context.getAuthenticator());
    Assert.assertTrue(context.getAuthenticator() instanceof ClassLoaderInContextAuthenticator);
    ClassLoaderInContextAuthenticator clAuthenticator = (ClassLoaderInContextAuthenticator) context.getAuthenticator();
    Assert.assertTrue(clAuthenticator.getDelegateAuthenticator() instanceof AsterAuthenticator);
    AsterServiceImpl service = ((AsterAuthenticator)clAuthenticator.getDelegateAuthenticator()).getService();
    Assert.assertEquals(AsterRestConfig.SubjectType.DC, service.getConfig().getAsterRestConfig().getSubjectType());
    Assert.assertEquals("id", service.getConfig().getAsterRestConfig().getClientId());
    Assert.assertEquals("version", service.getConfig().getAsterRestConfig().getClientVersion());
    Assert.assertEquals(engineConf, service.getConfig().getEngineConfig());
  }

}
