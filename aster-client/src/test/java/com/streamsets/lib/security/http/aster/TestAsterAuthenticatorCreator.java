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

import com.streamsets.datacollector.http.AsterAuthenticatorConfig;
import com.streamsets.datacollector.util.Configuration;
import org.eclipse.jetty.security.Authenticator;
import org.junit.Assert;
import org.junit.Test;

public class TestAsterAuthenticatorCreator {

  @Test
  public void testCreate() {
    AsterAuthenticatorCreator creator = new AsterAuthenticatorCreator();
    Configuration engineConf = new Configuration();
    engineConf.set("aster.url", "https://streamsets.dev:18632");
    AsterAuthenticatorConfig config = new AsterAuthenticatorConfig(
        "DC",
        "version",
        "id",
        engineConf,
        "/tmp"
    );
    Authenticator authenticator = creator.apply(config);

    Assert.assertNotNull(authenticator);
    Assert.assertTrue(authenticator instanceof ClassLoaderInContextAuthenticator);
    ClassLoaderInContextAuthenticator clAuthenticator = (ClassLoaderInContextAuthenticator) authenticator;
    Assert.assertTrue(clAuthenticator.getDelegateAuthenticator() instanceof AsterAuthenticator);
    AsterService service = ((AsterAuthenticator)clAuthenticator.getDelegateAuthenticator()).getService();
    Assert.assertEquals(AsterRestConfig.SubjectType.DC, service.getConfig().getAsterRestConfig().getSubjectType());
    Assert.assertEquals("id", service.getConfig().getAsterRestConfig().getClientId());
    Assert.assertEquals("version", service.getConfig().getAsterRestConfig().getClientVersion());
    Assert.assertEquals(engineConf, service.getConfig().getEngineConfig());
  }

}
