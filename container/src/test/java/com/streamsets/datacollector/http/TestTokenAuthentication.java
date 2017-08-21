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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@SuppressWarnings("unchecked")
public class TestTokenAuthentication {
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
  }

  @After
  public void cleanup() {
    stopServer();
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR);
  }

  private static String startServer(String authenticationType) throws  Exception {
    int port = NetworkUtils.getRandomPort();

    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTP_PORT_KEY, port);
    conf.set(WebServerTask.AUTHENTICATION_KEY, authenticationType);
    Writer writer = writer = new FileWriter(new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX +
      RuntimeInfo.CONFIG_DIR), "sdc.properties"));
    conf.save(writer);
    writer.close();


    File realmFile = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX +
      RuntimeInfo.CONFIG_DIR), authenticationType + "-realm.properties");
    writer = new FileWriter(realmFile);
    writer.write("admin: admin,user,admin\n");
    writer.write("multiRoleUser: multiRoleUser,user,creator,manager\n");
    writer.close();
    Files.setPosixFilePermissions(realmFile.toPath(), ImmutableSet.of(PosixFilePermission.OWNER_EXECUTE,
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

  @Test
  public void testFormTokenAuthentication() throws Exception {
    String userInfoURI =  startServer("form") + "/rest/v1/system/info/currentUser";

    //With out token
    Response response = ClientBuilder
      .newClient()
      .target(userInfoURI)
      .request()
      .get();
    Assert.assertEquals(404, response.getStatus());

    //With token
    Map<String, String> authenticationTokens = runtimeInfo.getAuthenticationTokens();
    response = ClientBuilder
      .newClient()
      .target(userInfoURI)
      .queryParam(ProxyAuthenticator.AUTH_USER, "admin")
      .queryParam(ProxyAuthenticator.AUTH_TOKEN, authenticationTokens.get("admin"))
      .request()
      .get();
    Assert.assertEquals(200, response.getStatus());

    Map userInfo = response.readEntity(Map.class);
    Assert.assertTrue(userInfo.containsKey("user"));
    Assert.assertEquals("admin", userInfo.get("user"));
    Assert.assertTrue(userInfo.containsKey("roles"));
    List<String> roles = (List<String>)userInfo.get("roles");
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals("admin", roles.get(0));


    //With Multi User token
    response = ClientBuilder
      .newClient()
      .target(userInfoURI)
      .queryParam(ProxyAuthenticator.AUTH_USER, "multiRoleUser")
      .queryParam(ProxyAuthenticator.AUTH_TOKEN, authenticationTokens.get("creator") + "," + authenticationTokens.get("manager"))
      .request()
      .get();
    Assert.assertEquals(200, response.getStatus());

    userInfo = response.readEntity(Map.class);
    Assert.assertTrue(userInfo.containsKey("user"));
    Assert.assertEquals("multiRoleUser", userInfo.get("user"));
    Assert.assertTrue(userInfo.containsKey("roles"));
    roles = (List<String>)userInfo.get("roles");
    Assert.assertEquals(2, roles.size());
    Assert.assertEquals("manager", roles.get(0));
    Assert.assertEquals("creator", roles.get(1));
  }

  @Test
  public void testBasicTokenAuthentication() throws Exception {
    String userInfoURI = startServer("basic") + "/rest/v1/info/user";

    //With out token
    Response response = ClientBuilder
      .newClient()
      .target(userInfoURI)
      .request()
      .get();
    Assert.assertEquals(401, response.getStatus());

    //TODO: Fix token authentication for Basic and Digest Type
    //With token
    /*Map<String, String> authenticationTokens = runtimeInfo.getAuthenticationTokens();
    response = ClientBuilder
      .newClient()
      .target(userInfoURI)
      .queryParam(ProxyAuthenticator.AUTH_USER, "admin")
      .queryParam(ProxyAuthenticator.AUTH_TOKEN, authenticationTokens.get("admin"))
      .request()
      .get();
    Assert.assertEquals(200, response.getState());

    Map userInfo = response.readEntity(Map.class);
    Assert.assertTrue(userInfo.containsKey("user"));
    Assert.assertEquals("admin", userInfo.get("user"));
    Assert.assertTrue(userInfo.containsKey("roles"));
    List<String> roles = (List<String>)userInfo.get("roles");
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals("admin", roles.get(0));
    */
  }
}
