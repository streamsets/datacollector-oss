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
import com.streamsets.lib.security.http.CORSConstants;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.testing.NetworkUtils;
import dagger.ObjectGraph;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.UUID;

public class TestHttpAccessControl {
  private static String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
  }

  private static class MockRegistrationServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  private static Server mockRegistrationServer;
  private static int registrationPort;
  private static String baseDir;
  private static Task server;
  private static String baseURL;
  private static RuntimeInfo runtimeInfo;

  @Before
  public void setup() throws Exception {
    registrationPort = NetworkUtils.getRandomPort();
    mockRegistrationServer = new Server(registrationPort);
    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.addServlet(
        new ServletHolder(new MockRegistrationServlet()),
        "/security/public-rest/v1/components/registration"
    );
    contextHandler.setContextPath("/");
    mockRegistrationServer.setHandler(contextHandler);
    mockRegistrationServer.start();


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

    /**
     * Tests sending of restricted headers (Origin and Access-Control-Request-Method) which are
     * used for CORS. These headers are by default skipped by the {@link java.net.HttpURLConnection}.
     * The system property {@code sun.net.http.allowRestrictedHeaders} must be defined in order to
     * allow these headers.
     */
     System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
  }

  @After
  public void cleanup() throws Exception {
    stopServer();
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR);
    if (mockRegistrationServer != null) {
      mockRegistrationServer.stop();
    }
  }

  private static String startServer(String authenticationType, boolean dpmEnabled) throws  Exception {
    int port = NetworkUtils.getRandomPort();

    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTP_PORT_KEY, port);
    conf.set(WebServerTask.AUTHENTICATION_KEY, authenticationType);
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "token");
    conf.set(RemoteSSOService.SECURITY_SERVICE_COMPONENT_ID_CONFIG, "token");
    conf.set(RemoteSSOService.DPM_ENABLED, dpmEnabled);
    conf.set(RemoteSSOService.DPM_BASE_URL_CONFIG, "http://localhost:" + registrationPort);

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
    runtimeInfo.setAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR,
        new URL("file://" + baseDir + "/log4j.properties"));

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
  public void testForFormAuthentication() throws Exception {
    String userInfoURI =  startServer("form", false) + "/rest/v1/system/info/currentUser";
    testPreFlightRequest(userInfoURI);

    // failing in jenkins
    testCORSGetRequest(userInfoURI);
  }

  @Test
  public void testForSSOAuthentication() throws Exception {
    String userInfoURI =  startServer("", true) + "/rest/v1/system/info/currentUser";
    testPreFlightRequest(userInfoURI);
  }

  @Test
  public void testDisabledTrace() throws Exception {
    String serverUrl =  startServer("form", false) + "/jmx";

    HttpURLConnection conn = (HttpURLConnection) new URL(serverUrl).openConnection();
    conn.setRequestMethod("TRACE");
    Assert.assertEquals(403, conn.getResponseCode());
  }

  /**
   * Browser "pre flighted" requests first send an HTTP request by the 'OPTIONS' method to the resource on the other
   * domain, in order to determine whether the actual request is safe to send.
   *
   * No authentication required for OPTIONS method
   *
   * @param userInfoURI URI
   */
  private void testPreFlightRequest(String userInfoURI) {
    Response response = ClientBuilder
        .newClient()
        .target(userInfoURI)
        .request()
        .options();

    Assert.assertEquals(200, response.getStatus());

    MultivaluedMap<String, Object> responseHeader = response.getHeaders();

    List<Object> allowOriginHeader = responseHeader.get(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER);
    Assert.assertNotNull(allowOriginHeader);
    Assert.assertEquals(1, allowOriginHeader.size());
    Assert.assertEquals(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT, allowOriginHeader.get(0));


    List<Object> allowHeadersHeader = responseHeader.get(CrossOriginFilter.ACCESS_CONTROL_ALLOW_HEADERS_HEADER);
    Assert.assertNotNull(allowHeadersHeader);
    Assert.assertEquals(1, allowHeadersHeader.size());
    Assert.assertEquals(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_HEADERS_DEFAULT, allowHeadersHeader.get(0));

    List<Object> allowMethodsHeader = responseHeader.get(CrossOriginFilter.ACCESS_CONTROL_ALLOW_METHODS_HEADER);
    Assert.assertNotNull(allowMethodsHeader);
    Assert.assertEquals(1, allowMethodsHeader.size());
    Assert.assertEquals(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_METHODS_DEFAULT, allowMethodsHeader.get(0));
  }


  private void testCORSGetRequest(String userInfoURI) throws Exception {
    HttpAuthenticationFeature authenticationFeature = HttpAuthenticationFeature.basic("admin", "admin");
    Response response = ClientBuilder.newClient()
        .target(userInfoURI)
        .register(authenticationFeature)
        .request()
        .header("Origin", "http://example.com")
        .header("Access-Control-Request-Method", "GET")
        .get();

    Assert.assertEquals(200, response.getStatus());

    MultivaluedMap<String, Object> responseHeader = response.getHeaders();

    List<Object> allowOriginHeader = responseHeader.get(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER);
    Assert.assertNotNull(allowOriginHeader);
    Assert.assertEquals(1, allowOriginHeader.size());
    Assert.assertEquals("http://example.com", allowOriginHeader.get(0));
  }

}
