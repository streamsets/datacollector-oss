/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.security;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.security.SecurityContext;
import com.streamsets.datacollector.util.Configuration;

import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.auth.Subject;

import java.io.File;
import java.util.Map;
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
    conf.set(SecurityContext.KERBEROS_ENABLED_KEY, true);
    conf.set(SecurityContext.KERBEROS_KEYTAB_KEY, "test.keytab");

    String hostname = SecurityContext.getLocalHostName();

    Map<String, String> principals = ImmutableMap.<String, String>builder()
                                                 .put("foo", "foo")
                                                 .put("foo/bar","foo/bar")
                                                 .put("foo/bar@REALM", "foo/bar@REALM")
                                                 .put("foo/_HOST", "foo/" + hostname)
                                                 .put("foo/_HOST@REALM", "foo/" + hostname + "@REALM")
                                                 .put("foo/0.0.0.0", "foo/" + hostname)
                                                 .put("foo/0.0.0.0@REALM", "foo/" + hostname + "@REALM").build();

    for (Map.Entry<String, String> entry : principals.entrySet()) {
      conf.set(SecurityContext.KERBEROS_PRINCIPAL_KEY, entry.getKey());
      SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
      Assert.assertEquals(entry.getValue(), context.getKerberosPrincipal());
    }
  }

  @Test(expected = RuntimeException.class)
  public void invalidLogin() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SecurityContext.KERBEROS_ENABLED_KEY, true);
    conf.set(SecurityContext.KERBEROS_PRINCIPAL_KEY, "foo");
    conf.set(SecurityContext.KERBEROS_KEYTAB_KEY, "foo.keytab");
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    Assert.assertTrue(context.isKerberosEnabled());
    context.login();
  }

  @Test
  public void notLoggedIn() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SecurityContext.KERBEROS_ENABLED_KEY, true);
    conf.set(SecurityContext.KERBEROS_PRINCIPAL_KEY, "foo");
    conf.set(SecurityContext.KERBEROS_KEYTAB_KEY, "test.keytab");
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    Assert.assertNull(context.getSubject());
  }

  @Test
  public void loginKerberosDisabled() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SecurityContext.KERBEROS_ENABLED_KEY, false);
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    Assert.assertFalse(context.isKerberosEnabled());
    context.login();
    Subject subject = context.getSubject();
    Assert.assertNotNull(subject);
    System.out.println(subject);
    context.logout();
  }

  private void loginFromKeytab(String keytab) throws Exception {
    Configuration conf = new Configuration();
    conf.set(SecurityContext.KERBEROS_ENABLED_KEY, true);
    conf.set(SecurityContext.KERBEROS_PRINCIPAL_KEY, "foo");
    conf.set(SecurityContext.KERBEROS_KEYTAB_KEY, keytab);
    SecurityContext context = new SecurityContext(getMockRuntimeInfo(), conf);
    Assert.assertTrue(context.isKerberosEnabled());
    context.login();
    Subject subject = context.getSubject();
    Assert.assertNotNull(subject);
    System.out.println(subject);
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

}
