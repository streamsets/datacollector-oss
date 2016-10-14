/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.datacollector.http;

import com.google.common.io.Resources;
import com.streamsets.datacollector.main.MainStandalonePipelineManagerModule;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.testing.NetworkUtils;
import dagger.ObjectGraph;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.UUID;
import org.junit.Assert;


@RunWith(Parameterized.class)
public class LDAPAuthenticationIT {
  /**
   * This IT uses two docker containers to mimic two different LDAP servers.
   * When authentication failed on the 1st server, SDC will fallback to the 2nd server
   * and proceed authentication.
   */

  // Connection to Ldap Server 1, authentication will fail on most of the tests
  private static LdapConnection connection1;
  // Connection to Ldap Server 2, authentication will success on most of the tests
  private static LdapConnection connection2;
  private static final int LDAP_PORT = 389;

  private static Logger LOG = LoggerFactory.getLogger(LDAPAuthenticationIT.class);

  @ClassRule
  public static GenericContainer server1 = new GenericContainer("osixia/openldap:1.1.6").withExposedPorts(LDAP_PORT);
  @ClassRule
  public static GenericContainer server2 = new GenericContainer("osixia/openldap:1.1.6").withExposedPorts(LDAP_PORT);

  // default bindDn and password for the docker osixia/openldap
  private static final String BIND_DN = "cn=admin,dc=example,dc=org";
  private static final String BIND_PWD = "admin";

  private static Task server;
  private static String sdcURL;
  private static String confDir = "target/" + UUID.randomUUID().toString();

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

  public LDAPAuthenticationIT(String username, String password, boolean result, List<String> role){
    this.username = username;
    this.password = password;
    this.result = result;
    this.role = role;
  }

  @BeforeClass
  public static void setUpClass() throws Exception{
    // create conf dir
    new File(confDir).mkdirs();

    performLdapAdd();
    startServer();
  }

  @AfterClass
  public static void cleanUpClass() throws IOException {
    if (server != null) {
      server.stop();
    }
    connection1.close();
    connection2.close();
    server1.stop();
    server2.stop();
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
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

  private static void startServer()  throws Exception {
    int port = NetworkUtils.getRandomPort();
    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTP_PORT_KEY, port);
    conf.set(WebServerTask.AUTHENTICATION_KEY, "basic");
    conf.set(WebServerTask.HTTP_AUTHENTICATION_LOGIN_MODULE, "ldap");
    conf.set(WebServerTask.HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING,
        "managers:manager;engineering:creator;finance:admin;");
    Writer writer;

    writer = new FileWriter(new File(confDir, "sdc.properties"));
    conf.save(writer);
    writer.close();

    File realmFile = new File(confDir, "ldap-login.conf");
    writer = new FileWriter(realmFile);
    writer.write("ldap {\n" + // information for server 1
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
        "};");
    writer.close();

    // Start SDC
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, confDir);
    ObjectGraph dagger = ObjectGraph.create(MainStandalonePipelineManagerModule.class);
    server = dagger.get(TaskWrapper.class);
    server.init();
    server.run();
    sdcURL = "http://localhost:" + Integer.toString(port);
    LOG.debug("server={}", sdcURL);
  }

  private static void performLdapAdd(){
    // setup Ldap server 1
    connection1 = new LdapNetworkConnection(server1.getContainerIpAddress(), server1.getMappedPort(LDAP_PORT));
    try {
      connection1.bind(BIND_DN, BIND_PWD);
      LdifReader reader = new LdifReader(Resources.getResource("ldap-server1-entries.ldif").getFile());
      for ( LdifEntry entry : reader) {
        connection1.add(entry.getEntry());
      }
    } catch (LdapException e){
      LOG.error("Setup server 1 failed " + e);
    }

    // Setup ldap server 2
    connection2 = new LdapNetworkConnection(server2.getContainerIpAddress(), server2.getMappedPort(LDAP_PORT));
    try {
      connection2.bind(BIND_DN, BIND_PWD);
      LdifReader reader = new LdifReader(Resources.getResource("ldap-server2-entries.ldif").getFile());
      for ( LdifEntry entry : reader) {
        connection2.add(entry.getEntry());
      }
    } catch (LdapException e){
      LOG.error("Setup server 2 failed " + e);
    }
  }
}
