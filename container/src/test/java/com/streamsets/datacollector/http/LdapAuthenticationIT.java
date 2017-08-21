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

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
@Ignore

public class LdapAuthenticationIT extends LdapAuthenticationBaseIT  {
  // Connection to Ldap Server
  private static LdapConnection connection;

  @ClassRule
  public static GenericContainer server = new GenericContainer("osixia/openldap:1.1.6").withExposedPorts(LDAP_PORT);

  @BeforeClass
  public static void setUpClass() throws Exception {
    // create conf dir
    new File(confDir).mkdirs();
    connection = setupLdapServer(server, "ldap-server2-entries.ldif");
  }

  @AfterClass
  public static void cleanUpClass() throws IOException {
    connection.close();
    if (server != null) {
      server.stop();
    }
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
  }

  /**
   * Test the default behavior using existing configuration. Default behavior is searching
   * role by full DN. User needs to provide roleMemberAttribute and roleObjectClass,
   * and generate a search filter by (&(roleObjectClass=roleObjectClass)(roleMemberAttribute='user's full DN'))
   * @throws Exception
   */
  @Test
  public void testDefaultBehavior() throws Exception {
    String originalConf = "ldap {\n" + // information for server 1
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"" + server.getContainerIpAddress()+ "\"\n" +
        "  port=\"" + server.getMappedPort(LDAP_PORT) + "\"\n" +
        "  bindDn=\"" + BIND_DN + "\"\n" +
        "  bindPassword=\"" + BIND_PWD + "\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"false\"\n" +
        "  userBaseDn=\"ou=employees,dc=example,dc=org\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=departments,dc=example,dc=org\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"member\"\n" +
        "  roleObjectClass=\"groupOfNames\"\n" +
        "  roleFilter=\"\";\n" +   // roleSearchFilter is empty
        "};";
    assertAuthenticationSuccess(originalConf, "user4", "user4");
  }

  /**
   * Test using search filter. If roleSearchFilter is provided and has {user}, then
   * we apply the filter by replacing {user} with UID.
   * @throws Exception
   */
  @Test
  public void testRoleFilterMemberUidForceBindingFalse() throws Exception {
    String memberUidFilter = "ldap {\n" + // information for server 1
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"" + server.getContainerIpAddress()+ "\"\n" +
        "  port=\"" + server.getMappedPort(LDAP_PORT) + "\"\n" +
        "  bindDn=\"" + BIND_DN + "\"\n" +
        "  bindPassword=\"" + BIND_PWD + "\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"false\"\n" +
        "  userBaseDn=\"ou=employees,dc=example,dc=org\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=departments,dc=example,dc=org\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"memberUid\"\n" +
        "  roleObjectClass=\"posixGroup\"\n" +
        "  roleFilter=\"memberUid={user}\";\n" +
        "};";

    assertAuthenticationSuccess(memberUidFilter, "user6", "user6");
  }

  /**
   * Test using search filter. If roleSearchFilter is provided and has {user}, then
   * we apply the filter by replacing {user} with UID.
   * @throws Exception
   */
  @Test
  public void testRoleFilterMemberUidForceBindingTrue() throws Exception {
    String memberUidFilter = "ldap {\n" + // information for server 1
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"" + server.getContainerIpAddress()+ "\"\n" +
        "  port=\"" + server.getMappedPort(LDAP_PORT) + "\"\n" +
        "  bindDn=\"" + BIND_DN + "\"\n" +
        "  bindPassword=\"" + BIND_PWD + "\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"true\"\n" +
        "  userBaseDn=\"ou=employees,dc=example,dc=org\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=departments,dc=example,dc=org\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"memberUid\"\n" +
        "  roleObjectClass=\"posixGroup\"\n" +
        "  roleFilter=\"memberUid={user}\";\n" +
        "};";

    assertAuthenticationSuccess(memberUidFilter, "user6", "user6");
  }


  /**
   * Test using search filter. If roleSearchFilter is provided and has {dn}, then
   * we apply the filter by replacing {dn} with user's full DN.
   * @throws Exception
   */
  @Test
  public void testRoleFilterMember() throws Exception {
    String memberFilter = "ldap {\n" + // information for server 1
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"" + server.getContainerIpAddress()+ "\"\n" +
        "  port=\"" + server.getMappedPort(LDAP_PORT) + "\"\n" +
        "  bindDn=\"" + BIND_DN + "\"\n" +
        "  bindPassword=\"" + BIND_PWD + "\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"false\"\n" +
        "  userBaseDn=\"ou=employees,dc=example,dc=org\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=departments,dc=example,dc=org\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleObjectClass=\"groupOfNames\"\n" +
        "  roleFilter=\"member={dn}\";\n" +
        "};";

    assertAuthenticationSuccess(memberFilter, "user4", "user4");
  }

  /**
   * Test using search filter. If roleSearchFilter is provided and has {dn}, then
   * we apply the filter by replacing {dn} with user's full DN.
   * @throws Exception
   */
  @Test
  public void testWrongFilter() throws Exception {
    String memberFilter = "ldap {\n" + // information for server 1
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"" + server.getContainerIpAddress()+ "\"\n" +
        "  port=\"" + server.getMappedPort(LDAP_PORT) + "\"\n" +
        "  bindDn=\"" + BIND_DN + "\"\n" +
        "  bindPassword=\"" + BIND_PWD + "\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"false\"\n" +
        "  userBaseDn=\"ou=employees,dc=example,dc=org\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=departments,dc=example,dc=org\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"member\"\n" +
        "  roleObjectClass=\"groupOfNames\"\n" +
        "  roleFilter=\"member={}\";\n" +   // {} is invalid. Default(search by full DN) should be applied
        "};";

    assertAuthenticationSuccess(memberFilter, "user4", "user4");
  }


  /**
   * Test using search filter. If roleSearchFilter is provided and has {dn}, then
   * we apply the filter by replacing {dn} with user's full DN.
   * @throws Exception
   */
  @Test
  public void testEmptyPassword() throws Exception {
    String memberFilter = "ldap {\n" + // information for server 1
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"" + server.getContainerIpAddress()+ "\"\n" +
        "  port=\"" + server.getMappedPort(LDAP_PORT) + "\"\n" +
        "  bindDn=\"" + BIND_DN + "\"\n" +
        "  bindPassword=\"" + BIND_PWD + "\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"false\"\n" +
        "  userBaseDn=\"ou=employees,dc=example,dc=org\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=departments,dc=example,dc=org\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"member\"\n" +
        "  roleObjectClass=\"groupOfNames\"\n" +
        "  roleFilter=\"member={dn}\";\n" +   // {} is invalid. Default(search by full DN) should be applied
        "};";

    startSDCServer(memberFilter);

    String userInfoURI = sdcURL  + "/rest/v1/system/info/currentUser";
    HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic("user4", "");
    Response response = ClientBuilder
        .newClient()
        .register(feature)
        .target(userInfoURI)
        .request()
        .get();

    Assert.assertEquals(401, response.getStatus());
  }

  /**
   * Test that we can authenticate users that are more than one level below the baseUserDN in the tree.
   * @throws Exception
   */
  @Test
  public void testSubtreeUserAuthentication() throws Exception {
    String originalConf = "ldap {\n" + // information for server 1
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"" + server.getContainerIpAddress()+ "\"\n" +
        "  port=\"" + server.getMappedPort(LDAP_PORT) + "\"\n" +
        "  bindDn=\"" + BIND_DN + "\"\n" +
        "  bindPassword=\"" + BIND_PWD + "\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"false\"\n" +
        "  userBaseDn=\"ou=employees,dc=example,dc=org\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=departments,dc=example,dc=org\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"member\"\n" +
        "  roleObjectClass=\"groupOfNames\"\n" +
        "  roleFilter=\"\";\n" +   // roleSearchFilter is empty
        "};";
    assertAuthenticationSuccess(originalConf, "internalUser1", "internalUser1Password");
  }

  /**
   * Assert authentication result using ldap-login.conf with given username and password.
   * All user for this test is configured so that role "admin" will be found and successfully authenticated.
   * @param ldapConf  The configuration for ldap-login.conf
   * @param username  username to login
   * @param password  password to login
   * @throws Exception
   */
  public static void assertAuthenticationSuccess(String ldapConf, String username, String password) throws Exception{
    startSDCServer(ldapConf);

    String userInfoURI = sdcURL  + "/rest/v1/system/info/currentUser";
    HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(username, password);
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
    Assert.assertEquals(1, ((List)userInfo.get("roles")).size());
    Assert.assertEquals("admin", ((List)userInfo.get("roles")).get(0));
    stopSDCServer();
  }
}
