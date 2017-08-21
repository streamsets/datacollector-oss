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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.testing.NetworkUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.UUID;

public class TestDisconnectedSSOManager {
  private String dataDir;

  @Before
  public void setup() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    File disconnectedCredentials = new File(dir, DisconnectedSSOManager.DISCONNECTED_SSO_AUTHENTICATION_FILE);

    Configuration conf = new Configuration();
    conf.set(PasswordHasher.ITERATIONS_KEY, 1);
    PasswordHasher hasher = new PasswordHasher(conf);

    DisconnectedSecurityInfo info = new DisconnectedSecurityInfo();
    info.addEntry("admin@org",
        hasher.getPasswordHash("admin@org", "admin"),
        ImmutableList.of("datacollector:admin", "user"),
        Collections.<String>emptyList()
    );
    info.addEntry("guest@org",
        hasher.getPasswordHash("guest@org", "guest"),
        ImmutableList.of("datacollector:guest", "user"),
        Collections.<String>emptyList()
    );
    info.toJsonFile(disconnectedCredentials);
    dataDir = dir.getAbsolutePath();
  }

  @Test
  public void testLifecyle() throws Exception {
    DisconnectedSSOManager manager = new DisconnectedSSOManager(dataDir, new Configuration());
    Assert.assertNotNull(manager.getAuthentication());
    Assert.assertNotNull(manager.getSsoService());
    Assert.assertFalse(manager.isEnabled());
    manager.setEnabled(true);
    Assert.assertTrue(manager.isEnabled());

    ServletContextHandler contextHandler = Mockito.mock(ServletContextHandler.class);
    manager.registerResources(contextHandler);

    ArgumentCaptor<ServletHolder> servletHolder = ArgumentCaptor.forClass(ServletHolder.class);
    ArgumentCaptor<String> path = ArgumentCaptor.forClass(String.class);
    Mockito.verify(contextHandler, Mockito.times(3)).addServlet(servletHolder.capture(), path.capture());
    Assert.assertEquals(DisconnectedSSOManager.SECURITY_RESOURCES, path.getAllValues().get(0));
    Assert.assertEquals(DisconnectedLoginServlet.URL_PATH, path.getAllValues().get(1));
    Assert.assertEquals(DisconnectedLogoutServlet.URL_PATH, path.getAllValues().get(2));

    ArgumentCaptor<String> attrName = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Object> attrValue = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(contextHandler, Mockito.times(2)).setAttribute(attrName.capture(), attrValue.capture());
    Assert.assertEquals(
        DisconnectedSSOManager.DISCONNECTED_SSO_AUTHENTICATION_HANDLER_ATTR,
        attrName.getAllValues().get(0)
    );
    Assert.assertEquals(DisconnectedSSOManager.DISCONNECTED_SSO_SERVICE_ATTR, attrName.getAllValues().get(1));
    Assert.assertTrue(attrValue.getAllValues().get(0) instanceof AuthenticationResourceHandler);
    Assert.assertTrue(attrValue.getAllValues().get(1) instanceof DisconnectedSSOService);
  }

  @Test
  public void testResources() throws Exception {
    DisconnectedSSOManager manager = new DisconnectedSSOManager(dataDir, new Configuration());

    int port = NetworkUtils.getRandomPort();
    Server server = new Server(port);
    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.setContextPath("/");
    manager.registerResources(contextHandler);

    //dummy login page to assert forwarding
    contextHandler.addServlet(new ServletHolder(new HttpServlet() {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
      }
    }), DisconnectedLoginServlet.DISCONNECTED_LOGIN_HTML);

    server.setHandler(contextHandler);
    server.start();
    String baseUrl = "http://localhost:" + port;
    try {
      //disable, all resources return UNAVAILABLE
      HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + "/security/login").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, conn.getResponseCode());
      conn = (HttpURLConnection) new URL(baseUrl + "/security/_logout").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, conn.getResponseCode());
      conn = (HttpURLConnection) new URL(baseUrl + "/security/public-rest/v1/authentication/login").openConnection();
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      Assert.assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, conn.getResponseCode());

      //enabled, resources should work
      manager.setEnabled(true);

      LoginJson login = new LoginJson();
      login.setUserName("admin@org");
      login.setPassword("admin");
      conn = (HttpURLConnection) new URL(baseUrl + "/security/login").openConnection();
      conn.setRequestProperty(SSOConstants.X_REST_CALL, "foo");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      conn = (HttpURLConnection) new URL(baseUrl + "/security/_logout").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      //login OK
      conn = (HttpURLConnection) new URL(baseUrl + "/security/public-rest/v1/authentication/login").openConnection();
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json");
      new ObjectMapper().writeValue(conn.getOutputStream(), login);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      //login INCORRECT
      login.setPassword("invalid");
      conn = (HttpURLConnection) new URL(baseUrl + "/security/public-rest/v1/authentication/login").openConnection();
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json");
      new ObjectMapper().writeValue(conn.getOutputStream(), login);
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());

    } finally {
      server.stop();
    }
  }

}
