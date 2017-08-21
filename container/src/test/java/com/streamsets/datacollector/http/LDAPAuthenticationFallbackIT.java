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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Arrays;


@Ignore
@RunWith(Parameterized.class)
public class LDAPAuthenticationFallbackIT extends LdapAuthenticationBaseIT {
  /**
   * This IT uses two docker containers to mimic two different LDAP servers.
   * When authentication failed on the 1st server, SDC will fallback to the 2nd server
   * and proceed authentication.
   */

  // Connection to Ldap Server 1, authentication will fail on most of the tests
  private static LdapConnection connection1;
  // Connection to Ldap Server 2, authentication will success on most of the tests
  private static LdapConnection connection2;

  @ClassRule
  public static GenericContainer server1 = new GenericContainer("osixia/openldap:1.1.6").withExposedPorts(LDAP_PORT);
  @ClassRule
  public static GenericContainer server2 = new GenericContainer("osixia/openldap:1.1.6").withExposedPorts(LDAP_PORT);


  @Parameterized.Parameters
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][]{
        // {username, password, authentication success?, expected sdc role to be found}
        {"user1", "user1", true, ImmutableList.of("creator")}, // wrong password in server1, but correct in server2
        {"user2", "user2", true, ImmutableList.of("admin")}, // user not found in server1, but found in server2
        {"user3", "user3", true, ImmutableList.of("admin")}, // group is not mapped in server1
        {"user4", "user4", true, ImmutableList.of("admin")}, // no group in server1, but found in server2
        {"user3", "dummy", false, ImmutableList.of()}, // password is wrong. Auth failed on both servers
        {"user5", "user5", true, ImmutableList.of("manager", "admin")} // user found in both servers but roles are different
    });
  }

  private String username;
  private String password;
  private boolean result;
  private List<String> role;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // create conf dir
    new File(confDir).mkdirs();
    connection1 = setupLdapServer(server1, "ldif/ldap-server1-entries.ldif");
    connection2 = setupLdapServer(server2, "ldif/ldap-server2-entries.ldif");
    String multipleLdapConf = "ldap {\n" + // information for server 1
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"" + server1.getContainerIpAddress()+ "\"\n" +
        "  port=\"" + server1.getMappedPort(LDAP_PORT) + "\"\n" +
        "  bindDn=\"" + BIND_DN + "\"\n" +
        "  bindPassword=\"" + BIND_PWD + "\"\n" +
        "  authenticationMethod=\"simple\"\n" +
        "  forceBindingLogin=\"false\"\n" +
        "  userBaseDn=\"ou=users,dc=example,dc=org\"\n" +
        "  userRdnAttribute=\"uid\"\n" +
        "  userIdAttribute=\"uid\"\n" +
        "  userPasswordAttribute=\"userPassword\"\n" +
        "  userObjectClass=\"inetOrgPerson\"\n" +
        "  roleBaseDn=\"ou=groups,dc=example,dc=org\"\n" +
        "  roleNameAttribute=\"cn\"\n" +
        "  roleMemberAttribute=\"member\"\n" +
        "  roleObjectClass=\"groupOfNames\";\n" +
        "  \n" + // information for sever2
        "  com.streamsets.datacollector.http.LdapLoginModule required\n" +
        "  debug=\"false\"\n" +
        "  useLdaps=\"false\"\n" +
        "  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"\n" +
        "  hostname=\"" + server2.getContainerIpAddress()+ "\"\n" +
        "  port=\"" + server2.getMappedPort(LDAP_PORT) + "\"\n" +
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
        "  roleObjectClass=\"groupOfNames\";\n" +
        "};";

    startSDCServer(multipleLdapConf);
  }

  @AfterClass
  public static void cleanUpClass() throws IOException {
    connection1.close();
    connection2.close();
    server1.stop();
    server2.stop();
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    stopSDCServer();
  }

  public LDAPAuthenticationFallbackIT(String username, String password, boolean result, List<String> role)
  throws Exception {
    this.username = username;
    this.password = password;
    this.result = result;
    this.role = role;
  }

  @Test
  public void testLdapAuthentication(){
    String userInfoURI = sdcURL  + "/rest/v1/system/info/currentUser";
    HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(username, password);
    Response response = ClientBuilder
        .newClient()
        .register(feature)
        .target(userInfoURI)
        .request()
        .get();

    if (!result) {
      Assert.assertEquals(401, response.getStatus());
    } else{
      Assert.assertEquals(200, response.getStatus());
      Map userInfo = response.readEntity(Map.class);
      Assert.assertTrue(userInfo.containsKey("user"));
      Assert.assertEquals(username, userInfo.get("user"));
      Assert.assertTrue(userInfo.containsKey("roles"));
      List<String> roles = (List<String>) userInfo.get("roles");
      Assert.assertEquals(role.size(), roles.size());
      for(int i = 0; i < roles.size(); i++) {
        Assert.assertEquals(role.get(i), roles.get(i));
      }
    }
  }
}
