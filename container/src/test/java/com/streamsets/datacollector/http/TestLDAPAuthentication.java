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


import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.main.MainStandalonePipelineManagerModule;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.testing.NetworkUtils;
import dagger.ObjectGraph;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RunWith(FrameworkRunner.class)
@CreateLdapServer(
    transports =
        {
            @CreateTransport(protocol = "LDAP")
        })
public class TestLDAPAuthentication extends AbstractLdapTestUnit {

  private static Logger LOG = LoggerFactory.getLogger(TestLDAPAuthentication.class);

  private static String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
  }

  private static String baseDir;
  private static Task server;
  private static String baseURL;
  private static RuntimeInfo runtimeInfo;

  @Before
  public void setup() throws Exception {
    System.clearProperty(WebServerTask.JAVA_SECURITY_AUTH_LOGIN_CONFIG);
    server = null;
    baseDir = createTestDir();
    Assert.assertTrue(new File(baseDir, "etc").mkdir());
    Assert.assertTrue(new File(baseDir, "data").mkdir());
    Assert.assertTrue(new File(baseDir, "log").mkdir());
    Assert.assertTrue(new File(baseDir, "web").mkdir());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, baseDir + "/etc");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, baseDir + "/data");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR, baseDir + "/log");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR, baseDir + "/web");

    //Create users, groups and assign users to group in LDAP server
    createPrincipal("admin1", "admin1");
    createPrincipal("manager", "manager");
    createPrincipal("creator", "creator");
    createPrincipal("guest", "guest");

    createGroup("Admins", "uid=admin1,ou=users,ou=system");
    createGroup("Managers", "uid=manager,ou=users,ou=system");
    createGroup("Creators", "uid=creator,ou=users,ou=system");
    createGroup("Guests", "uid=guest,ou=users,ou=system");
  }

  @After
  public void cleanup() {
    if (server.getStatus() == Task.Status.RUNNING) {
      stopServer();
    }
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR);
  }

  private static String startServer(String authenticationType, String ldapConf) throws  Exception {
    int port = NetworkUtils.getRandomPort();

    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTP_PORT_KEY, port);
    conf.set(WebServerTask.AUTHENTICATION_KEY, authenticationType);
    conf.set(WebServerTask.HTTP_AUTHENTICATION_LOGIN_MODULE, "ldap");
    conf.set(WebServerTask.HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING,
        "Admins:admin;Managers:manager;Creators:creator;Guests:guest");
    Writer writer = new FileWriter(new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX +
        RuntimeInfo.CONFIG_DIR), "sdc.properties"));
    conf.save(writer);
    writer.close();


    File ldapConfFile = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX +
        RuntimeInfo.CONFIG_DIR), "ldap-login.conf");
    writer = new FileWriter(ldapConfFile);
    writer.write(ldapConf);
    writer.close();

    Files.setPosixFilePermissions(ldapConfFile.toPath(), ImmutableSet.of(PosixFilePermission.OWNER_EXECUTE,
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE));

    ObjectGraph dagger = ObjectGraph.create(MainStandalonePipelineManagerModule.class);

    runtimeInfo = dagger.get(RuntimeInfo.class);
    runtimeInfo.setAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR, new URL("file://" + baseDir + "/log4j.properties"));

    server = dagger.get(TaskWrapper.class);
    server.init();
    server.run();

    return "http://127.0.0.1:" + port;
  }

  private static void stopServer() {
    if (server != null) {
      server.stop();
    }
  }

  private void createPrincipal(String principal, String password)
      throws Exception {
    DirectoryService ds = getService();
    String baseDn = "ou=users,ou=system";
    String content = "dn: uid=" + principal + "," + baseDn + "\n" +
        "objectClass: top\n" +
        "objectClass: person\n" +
        "objectClass: inetOrgPerson\n" +
        "cn: " + principal + "\n" +
        "sn: " + principal + "\n" +
        "uid: " + principal + "\n" +
        "userPassword: " + password;

    for (LdifEntry ldifEntry : new LdifReader(new StringReader(content))) {
      ds.getAdminSession().add(new DefaultEntry(ds.getSchemaManager(),
          ldifEntry.getEntry()));
    }
  }

  private void createGroup(String groupName, String memberDn) throws Exception {
    DirectoryService ds = getService();
    String baseDn = "ou=groups,ou=system";
    String content = "dn: cn=" + groupName + "," + baseDn + "\n" +
        "objectClass: top\n" +
        "objectClass: groupofnames\n" +
        "cn: " + groupName + "\n" +
        "description: " + groupName + "\n" +
        "member: " + memberDn;

    for (LdifEntry ldifEntry : new LdifReader(new StringReader(content))) {
      ds.getAdminSession().add(new DefaultEntry(ds.getSchemaManager(),
          ldifEntry.getEntry()));
    }
  }

  @Test
  public void testLDAPAuthenticationForm() throws Exception {
    testLDAPAuthentication("form");
  }

  @Test
  public void testLDAPAuthenticationBasic() throws Exception {
    testLDAPAuthentication("basic");
  }

  @Test
  public void testLDAPAuthenticationDigest() throws Exception {
    testLDAPAuthentication("digest");
  }

  private void testLDAPAuthentication(String authType) throws Exception {
    String single = "ldap {\n" +
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"localhost\"\n" +
        "  port=\"" + ldapServer.getPort() + "\"\n" +
        "  bindDn=\"uid=admin,ou=system\"\n" +
        "  bindPassword=\"secret\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"false\"\n" +
        "  userBaseDn=\"ou=users,ou=system\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=groups,ou=system\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"member\"\n" +
        "  roleObjectClass=\"groupofnames\";\n" +
        "};";
    try {
      String baseURL = startServer(authType, single);
      testAuthenticationAndRoleMapping(baseURL, authType, "admin1", "admin1", "admin");
      testAuthenticationAndRoleMapping(baseURL, authType, "manager", "manager", "manager");
      testAuthenticationAndRoleMapping(baseURL, authType, "creator", "creator", "creator");
      testAuthenticationAndRoleMapping(baseURL, authType, "guest", "guest", "guest");
      stopServer();
    } catch (Exception e) {
      LOG.debug("Ignoring exception", e);
    } finally {
      stopServer();
    }
  }

  @Test
  public void testMultipleLDAPAuthentication() throws Exception {
    String authType = "basic";
    String multiple = "ldap {\n" + // Incorrect ldap entry. This should fail
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"dummyhost\"\n" +   // hostname is dummy host. should cause timeout error
        "  port=\"" + ldapServer.getPort() + "\"\n" +
        "  bindDn=\"uid=admin,ou=system\"\n" +
        "  bindPassword=\"dummy\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"false\"\n" +
        "  userBaseDn=\"ou=users,ou=system\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=groups,ou=system\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"member\"\n" +
        "  roleObjectClass=\"groupofnames\";\n" +
        "  \n" +   // Correct ldap entry. This should succeed.
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"localhost\"\n" +
        "  port=\"" + ldapServer.getPort() + "\"\n" +
        "  bindDn=\"uid=admin,ou=system\"\n" +
        "  bindPassword=\"secret\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"false\"\n" +
        "  userBaseDn=\"ou=users,ou=system\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=groups,ou=system\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"member\"\n" +
        "  roleObjectClass=\"groupofnames\";\n" +
        "};";

    try {
      String baseURL = startServer(authType, multiple);
      // first entry in ldap-login.conf is host unreachable, but second entry has correct ldap info.
      // authentication should succeed.
      testAuthenticationAndRoleMapping(baseURL, authType, "admin1", "admin1", "admin");
      testAuthenticationAndRoleMapping(baseURL, authType, "manager", "manager", "manager");
      testAuthenticationAndRoleMapping(baseURL, authType, "creator", "creator", "creator");
      testAuthenticationAndRoleMapping(baseURL, authType, "guest", "guest", "guest");
      stopServer();
    } catch (Exception e) {
      LOG.debug("Ignoring exception", e);
    } finally {
      stopServer();
    }
  }

  @SuppressWarnings("unchecked")
  private void testAuthenticationAndRoleMapping(String baseURL, String  authType, String username, String password,
                                                String role) {
    String userInfoURI = baseURL  + "/rest/v1/system/info/currentUser";
    HttpAuthenticationFeature feature = null;

    switch(authType) {
      case "basic":
      case "form":
        feature = HttpAuthenticationFeature.basic(username, password);
        break;
      case "digest":
        feature = HttpAuthenticationFeature.digest(username, password);
        break;
    }

    Response response = ClientBuilder
        .newClient()
        .register(feature)
        .target(userInfoURI)
        .request()
        .get();
    Assert.assertEquals(200, response.getStatus());
    Map userInfo = response.readEntity(Map.class);
    Assert.assertTrue(userInfo.containsKey("user"));
    Assert.assertEquals(username, userInfo.get("user"));
    Assert.assertTrue(userInfo.containsKey("roles"));
    List<String> roles = (List<String>)userInfo.get("roles");
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals(role, roles.get(0));
  }

  @Test(expected = RuntimeException.class)
  public void testDigesLDAPForceBinding() throws Exception {
    String single = "ldap {\n" +
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"localhost\"\n" +
        "  port=\"" + ldapServer.getPort() + "\"\n" +
        "  bindDn=\"uid=admin,ou=system\"\n" +
        "  bindPassword=\"secret\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"true\"\n" +
        "  userBaseDn=\"ou=users,ou=system\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=groups,ou=system\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"member\"\n" +
        "  roleObjectClass=\"groupofnames\";\n" +
        "};";
    startServer("digest", single);
  }

}
