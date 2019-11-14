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
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import java.io.File;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
  }

  @Test
  public void notLoggedIn() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SecurityConfiguration.KERBEROS_ENABLED_KEY, true);
    conf.set(SecurityConfiguration.KERBEROS_PRINCIPAL_KEY, "foo");
    conf.set(SecurityConfiguration.KERBEROS_KEYTAB_KEY, "test.keytab");
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    Assert.assertNull(context.getSubject());
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
    context.logout();
  }

  @Test
  public void propertiesStillResolvedIfKerberosDisabled() throws Exception {
    final Configuration conf = new Configuration();
    conf.set(SecurityConfiguration.KERBEROS_ENABLED_KEY, false);
    conf.set(SecurityConfiguration.KERBEROS_ALWAYS_RESOLVE_PROPS_KEY, true);
    final String principal = "jeff@CLUSTER";
    final Path keytab = Files.createTempFile("jeff", ".keytab");
    keytab.toFile().deleteOnExit();
    conf.set(SecurityConfiguration.KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(SecurityConfiguration.KERBEROS_KEYTAB_KEY, keytab.toString());
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    Assert.assertFalse(context.getSecurityConfiguration().isKerberosEnabled());
    Assert.assertEquals(keytab.toString(), context.getSecurityConfiguration().getKerberosKeytab());
    Assert.assertEquals(principal, context.getSecurityConfiguration().getKerberosPrincipal());
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
    context.logout();
  }

  @Test
  public void testRenewalCalculation() {
    Configuration conf = new Configuration();
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    context = Mockito.spy(context);

    Assert.assertEquals(context.getRenewalWindow(), context.getRenewalWindow(), 0.001);
    Assert.assertTrue(context.getRenewalWindow() >= 0.5);
    Assert.assertTrue(context.getRenewalWindow() < 0.7);

    // brute force test
    for (int i = 0; i < 1000; i++) {
      double window = context.computeRenewalWindow();
      Assert.assertTrue(window >= 0.5);
      Assert.assertTrue(window < 0.7);
    }

    // renewal time
    Mockito.doReturn(0.5D).when(context).getRenewalWindow();
    Assert.assertEquals(10 + (long)(0.5D * (20 - 10)), context.getRenewalTime(10, 20), 0.001);
  }

  private KerberosTicket createMockTGT(String name, Date start, Date end) {
    KerberosPrincipal userPrincipal = new KerberosPrincipal(name);
    KerberosPrincipal serverPrincipal = new KerberosPrincipal("krbtgt/" + userPrincipal.getRealm());
    return new KerberosTicket(new byte[0], userPrincipal,  serverPrincipal, new byte[0], 0, new boolean[0],
        start, start, end, end, new InetAddress[0]);
  }

  @Test
  public void testGetKerberosTicket() {
    long now = System.currentTimeMillis();
    Date v1 = new Date(now + TimeUnit.DAYS.toMillis(1));
    Date v2 = new Date(now + TimeUnit.DAYS.toMillis(12));
    Date v3 = new Date(now + TimeUnit.DAYS.toMillis(5));
    KerberosTicket ticket = createMockTGT("short", v1, v1);
    KerberosTicket ticket2 = createMockTGT("long", v2, v2);
    KerberosTicket ticket3 = createMockTGT("medium", v3, v3);

    Configuration conf = new Configuration();
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    context = Mockito.spy(context);
    Mockito.doReturn(now).when(context).getTimeNow();

    Subject subject = new Subject();
    Mockito.doReturn(subject).when(context).getSubject();
    subject.getPrivateCredentials().add(ticket);
    subject.getPrivateCredentials().add(ticket2);
    subject.getPrivateCredentials().add(ticket3);

    Assert.assertEquals(ticket2, context.getNewestTGT());
  }

}
