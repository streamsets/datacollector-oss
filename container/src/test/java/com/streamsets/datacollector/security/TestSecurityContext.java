/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.security;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TestSecurityContext {
  private static File testDir;
  private static File keytabFile;
  private static MiniKdc miniKdc;

  @BeforeClass
  public static void startKdc() throws Exception {
    testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    File kdcDir = new File(testDir, "kdc");
    Assert.assertTrue(kdcDir.mkdirs());
    keytabFile = new File(testDir, "test.keytab");
    miniKdc = new MiniKdc(MiniKdc.createConf(), testDir);
    miniKdc.start();
    miniKdc.createPrincipal(keytabFile, "foo", "bar/localhost");
  }

  @AfterClass
  public static void stopKdc() {
    if (miniKdc != null) {
      miniKdc.stop();
      miniKdc = null;
    }
  }

  private RuntimeInfo getMockRuntimeInfo() {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getConfigDir()).thenReturn(testDir.getAbsolutePath());
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());
    return runtimeInfo;
  }

  @Test
  public void testPrincipalResolution() {
    Configuration conf = new Configuration();
    conf.set(SecurityConfiguration.KERBEROS_ENABLED_KEY, true);
    conf.set(SecurityConfiguration.KERBEROS_KEYTAB_KEY, "test.keytab");

    String hostname = SecurityConfiguration.getLocalHostName();

    Map<String, String> principals = ImmutableMap.<String, String>builder()
                                                 .put("foo", "foo")
                                                 .put("foo/bar","foo/bar")
                                                 .put("foo/bar@REALM", "foo/bar@REALM")
                                                 .put("foo/_HOST", "foo/" + hostname)
                                                 .put("foo/_HOST@REALM", "foo/" + hostname + "@REALM")
                                                 .put("foo/0.0.0.0", "foo/" + hostname)
                                                 .put("foo/0.0.0.0@REALM", "foo/" + hostname + "@REALM").build();

    for (Map.Entry<String, String> entry : principals.entrySet()) {
      conf.set(SecurityConfiguration.KERBEROS_PRINCIPAL_KEY, entry.getKey());
      SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
      Assert.assertEquals(entry.getValue(), context.getSecurityConfiguration().getKerberosPrincipal());
    }
  }

  @Test(expected = RuntimeException.class)
  public void invalidLogin() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SecurityConfiguration.KERBEROS_ENABLED_KEY, true);
    conf.set(SecurityConfiguration.KERBEROS_PRINCIPAL_KEY, "foo");
    conf.set(SecurityConfiguration.KERBEROS_KEYTAB_KEY, "foo.keytab");
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    Assert.assertTrue(context.getSecurityConfiguration().isKerberosEnabled());
    context.login();
    verifyCredentialCache(false);
  }

  @Test
  public void notLoggedIn() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SecurityConfiguration.KERBEROS_ENABLED_KEY, true);
    conf.set(SecurityConfiguration.KERBEROS_PRINCIPAL_KEY, "foo");
    conf.set(SecurityConfiguration.KERBEROS_KEYTAB_KEY, "test.keytab");
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    Assert.assertNull(context.getSubject());
    verifyCredentialCache(false);
  }

  @Test
  public void loginKerberosDisabled() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SecurityConfiguration.KERBEROS_ENABLED_KEY, false);
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    Assert.assertFalse(context.getSecurityConfiguration().isKerberosEnabled());
    context.login();
    Subject subject = context.getSubject();
    Assert.assertNotNull(subject);
    System.out.println(subject);
    // No credential file is expected as kerberos is disabled
    verifyCredentialCache(false);
    context.logout();
  }

  @Test
  public void loginFromAbsoluteKeytab() throws Exception {
    loginFromKeytab(keytabFile.getAbsolutePath());
  }

  @Test
  public void loginFromRelativeKeytab() throws Exception {
    loginFromKeytab(keytabFile.getName());
  }

  private void loginFromKeytab(String keytab) throws Exception {
    Configuration conf = new Configuration();
    conf.set(SecurityConfiguration.KERBEROS_ENABLED_KEY, true);
    conf.set(SecurityConfiguration.KERBEROS_PRINCIPAL_KEY, "foo");
    conf.set(SecurityConfiguration.KERBEROS_KEYTAB_KEY, keytab);
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    Assert.assertTrue(context.getSecurityConfiguration().isKerberosEnabled());
    context.login();
    Subject subject = context.getSubject();
    Assert.assertNotNull(subject);
    System.out.println(subject);
    // Expect a credential cache to exist
    verifyCredentialCache(true);
    context.logout();
    // Expect credential cache to be removed after logout
    verifyCredentialCache(false);
  }

  private void verifyCredentialCache(boolean expected) throws IOException {
    File ticketCache = new File(getMockRuntimeInfo().getDataDir(), "sdc-krb5.ticketCache");
    Assert.assertEquals(expected, ticketCache.exists());
    if(expected) {
      Set<PosixFilePermission> posixFilePermissions = Files.getPosixFilePermissions(ticketCache.toPath());
      Assert.assertEquals(2, posixFilePermissions.size());
      Assert.assertTrue(posixFilePermissions.contains(PosixFilePermission.OWNER_READ));
      Assert.assertTrue(posixFilePermissions.contains(PosixFilePermission.OWNER_WRITE));
    }
  }

}
