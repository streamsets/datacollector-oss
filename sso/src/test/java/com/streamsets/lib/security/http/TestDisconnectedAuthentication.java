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
package com.streamsets.lib.security.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.util.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.UUID;

public class TestDisconnectedAuthentication {
  private static File authInfoFile;

  @BeforeClass
  public static void setup() throws Exception {
    File testDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());
    authInfoFile = new File(testDir, "disconnectedssoauth.json");

    Configuration conf = new Configuration();
    conf.set(PasswordHasher.ITERATIONS_KEY, 1);
    PasswordHasher hasher = new PasswordHasher(conf);

    DisconnectedSecurityInfo info = new DisconnectedSecurityInfo();
    info.addEntry(
        "admin@org",
        hasher.getPasswordHash("admin@org", "admin"),
        ImmutableList.of("datacollector:admin", "user"),
        Collections.<String>emptyList()
    );
    info.addEntry(
        "guest@org",
        hasher.getPasswordHash("guest@org", "guest"),
        ImmutableList.of("datacollector:guest", "user"),
        Collections.<String>emptyList()
    );
    info.toJsonFile(authInfoFile);
  }

  @Test
  public void testNoCredentialsFile() throws Exception {
    DisconnectedAuthentication authentication = new DisconnectedAuthentication(new File(UUID.randomUUID().toString()));
    Assert.assertNull(authentication.validateUserCredentials("admin@org", "guest", "ip2"));
  }

    @Test
  public void testAuthentication() throws Exception {
    DisconnectedAuthentication authentication = new DisconnectedAuthentication(authInfoFile);
    authentication.reset();

    SSOPrincipal principal = authentication.validateUserCredentials("admin@org", "admin", "ip1");
    Assert.assertNotNull(principal);
    Assert.assertEquals("admin@org", principal.getPrincipalId());
    Assert.assertEquals("org", principal.getOrganizationId());
    Assert.assertEquals("-", principal.getPrincipalName());
    Assert.assertEquals("-", principal.getOrganizationName());
    Assert.assertEquals("-", principal.getEmail());
    Assert.assertEquals(-1, principal.getExpires());
    Assert.assertNotNull(UUID.fromString(principal.getTokenStr()));
      Assert.assertEquals(ImmutableSet.of(
          "datacollector:admin",
          "user",
          DisconnectedAuthentication.DISCONNECTED_MODE_ROLE
      ), principal.getRoles());
    Assert.assertTrue(principal.getAttributes().isEmpty());
    Assert.assertEquals("ip1", principal.getRequestIpAddress());

    principal = authentication.validateUserCredentials("guest@org", "guest", "ip2");
    Assert.assertNotNull(principal);
    Assert.assertEquals("guest@org", principal.getPrincipalId());
    Assert.assertEquals("org", principal.getOrganizationId());
      Assert.assertEquals(ImmutableSet.of(
          "datacollector:guest",
          "user",
          DisconnectedAuthentication.DISCONNECTED_MODE_ROLE
      ), principal.getRoles());

    Assert.assertNull(authentication.validateUserCredentials("admin@org", "guest", "ip2"));
  }

  @Test
  public void testSessions() throws Exception {
    DisconnectedAuthentication authentication = new DisconnectedAuthentication(authInfoFile);
    authentication.reset();

    SSOPrincipalJson principal = new SSOPrincipalJson();
    principal.setTokenStr("token");
    authentication.registerSession(principal);
    Assert.assertEquals(principal, authentication.getSessionHandler().get("token"));
  }

}
