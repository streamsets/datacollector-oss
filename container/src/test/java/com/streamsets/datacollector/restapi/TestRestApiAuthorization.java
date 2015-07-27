/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi;

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.log.LogUtils;
import com.streamsets.datacollector.main.MainStandalonePipelineManagerModule;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;

import dagger.ObjectGraph;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class TestRestApiAuthorization {

  private String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
  }

  private int getRandomPort() throws Exception {
    ServerSocket ss = new ServerSocket(0);
    int port = ss.getLocalPort();
    ss.close();
    return port;
  }

  private String log4jConf;
  private String baseDir;
  private Task server;

  @Before
  public void setup() throws Exception {
    server = null;
    baseDir = createTestDir();
    log4jConf = new File(baseDir, "log4j.properties").getAbsolutePath();
    File logFile = new File(baseDir, "x.log");
    Writer writer = new FileWriter(log4jConf);
    IOUtils.write(LogUtils.LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY + "=" + logFile.getAbsolutePath(), writer);
    writer.close();
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
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR);
  }

  private String startServer(boolean authzEnabled) throws  Exception {
    int port = getRandomPort();
    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTP_PORT_KEY, port);
    conf.set(WebServerTask.AUTHENTICATION_KEY, (authzEnabled) ? "basic" : "none");
    Writer writer = new FileWriter(new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR), "sdc.properties"));
    conf.save(writer);
    writer.close();
    File realmFile = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR), "basic-realm.properties");
    writer = new FileWriter(realmFile);
    IOUtils.copy(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("basic-realm.properties")),
                 writer);
    writer.close();
    Files.setPosixFilePermissions(realmFile.toPath(), WebServerTask.OWNER_PERMISSIONS);
    ObjectGraph dagger = ObjectGraph.create(MainStandalonePipelineManagerModule.class);
    RuntimeInfo runtimeInfo = dagger.get(RuntimeInfo.class);
    runtimeInfo.setAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR, new URL("file://" + baseDir + "/log4j.properties"));
    server = dagger.get(TaskWrapper.class);
    server.init();
    server.run();
    return "http://127.0.0.1:" + port;
  }

  private void stopServer() {
    if (server != null) {
      server.stop();
    }
  }

  enum Method { GET, POST, PUT, DELETE };

  private static final Set<String> ALL_ROLES = ImmutableSet
      .of(AuthzRole.ADMIN, AuthzRole.CREATOR, AuthzRole.MANAGER, AuthzRole.GUEST);

  static class RestApi {
    final String uriPath;
    final Method method;
    final Set<String> roles;

    public RestApi(String uriPath, Method method, String... roles) throws Exception {
      this.uriPath = uriPath;
      this.method = method;
      Set<String> set = new HashSet<>();
      if (roles != null) {
        for (String role : roles) {
          if (role != null) {
            set.add(role);
          }
        }
      }
      this.roles = Collections.unmodifiableSet(set);
    }
  }

  private void test(List<RestApi> apis, boolean authzEnabled) throws Exception {
    String baseUrl = startServer(authzEnabled);
    try {
      for (RestApi api : apis) {
        Set<String> has = api.roles;
        for (String user : ALL_ROLES) {
          user = "guest";
          URL url = new URL(baseUrl + api.uriPath);
          HttpURLConnection conn = (HttpURLConnection) url.openConnection();
          if (authzEnabled) {
            conn.setRequestProperty("Authorization", "Basic " + Base64.encodeBase64URLSafeString((user + ":" + user).getBytes()));
          }
          conn.setRequestMethod(api.method.name());
          conn.setDefaultUseCaches(false);
          if (authzEnabled) {
            if (has.contains(user)) {
              Assert.assertNotEquals(
                  Utils.format("Authz '{}' User '{}' METHOD '{}' API '{}'", authzEnabled, user, api.method, api.uriPath),
                  HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
            } else {
              Assert.assertEquals(
                  Utils
                      .format("Authz '{}' User '{}' METHOD '{}' API '{}'", authzEnabled, user, api.method, api.uriPath),
                  HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
            }
          } else {
            Assert.assertNotEquals(
                Utils.format("Authz '{}' User '{}' METHOD '{}' API '{}'", authzEnabled, user, api.method, api.uriPath),
                HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
          }
        }
      }
    } finally {
      stopServer();
    }
  }

  private List<RestApi> createRestApis() throws Exception {
    List<RestApi> list = new ArrayList<>();
    list.add(new RestApi("/rest/ping", Method.GET, AuthzRole.ALL_ROLES));

    list.add(new RestApi("/rest/v1/admin/shutdown", Method.POST, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/admin/threadsDump", Method.GET, AuthzRole.ADMIN));

    list.add(new RestApi("/rest/v1/configuration/ui", Method.GET, AuthzRole.ALL_ROLES));
    list.add(new RestApi("/rest/v1/configuration/all", Method.GET, AuthzRole.ALL_ROLES));

    list.add(new RestApi("/rest/v1/helpref", Method.GET, AuthzRole.ALL_ROLES));

    list.add(new RestApi("/rest/v1/info/sdc", Method.GET, AuthzRole.ALL_ROLES));
    list.add(new RestApi("/rest/v1/info/user", Method.GET, AuthzRole.ALL_ROLES));

    list.add(new RestApi("/rest/v1/logout", Method.POST, AuthzRole.ALL_ROLES));

    list.add(new RestApi("/rest/v1/pipelines/status", Method.GET, AuthzRole.ALL_ROLES));
    list.add(new RestApi("/rest/v1/pipeline/foo/status", Method.GET, AuthzRole.ALL_ROLES));
    list.add(new RestApi("/rest/v1/pipeline/foo/start", Method.POST, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/stop", Method.POST, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/resetOffset", Method.POST, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/snapshot/foo", Method.PUT, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipelines/snapshots", Method.GET, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/snapshots", Method.GET, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/snapshot/foo/status", Method.GET, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/snapshot/foo", Method.GET, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/snapshot/foo", Method.DELETE, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/metrics", Method.GET, AuthzRole.ALL_ROLES));
    list.add(new RestApi("/rest/v1/pipeline/foo/history", Method.GET, AuthzRole.ALL_ROLES));
    list.add(new RestApi("/rest/v1/pipeline/foo/history", Method.DELETE, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/errorRecords", Method.GET, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/errorMessages", Method.GET, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/sampledRecords", Method.GET, AuthzRole.MANAGER, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/alerts", Method.DELETE, AuthzRole.MANAGER, AuthzRole.ADMIN));

    list.add(new RestApi("/rest/v1/pipeline-library", Method.GET, AuthzRole.ALL_ROLES));
    list.add(new RestApi("/rest/v1/pipeline-library/foo", Method.GET, AuthzRole.ALL_ROLES));
    list.add(new RestApi("/rest/v1/pipeline-library/foo", Method.PUT, AuthzRole.CREATOR, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline-library/foo", Method.DELETE, AuthzRole.CREATOR, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline-library/foo", Method.POST, AuthzRole.CREATOR, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline-library/foo/rules", Method.GET, AuthzRole.ALL_ROLES));
    list.add(new RestApi("/rest/v1/pipeline-library/foo/rules", Method.POST,AuthzRole.CREATOR, AuthzRole.ADMIN,
                         AuthzRole.MANAGER));

    list.add(new RestApi("/rest/v1/preview/foo/create", Method.POST, AuthzRole.CREATOR, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/preview-id/uuid/status", Method.GET, AuthzRole.CREATOR, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/preview-id/uuid", Method.GET, AuthzRole.CREATOR, AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/preview-id/uuid/cancel", Method.POST, AuthzRole.CREATOR, AuthzRole.ADMIN));

    list.add(new RestApi("/rest/v1/preview/foo/rawSourcePreview", Method.GET, AuthzRole.CREATOR,
                         AuthzRole.ADMIN));
    list.add(new RestApi("/rest/v1/pipeline/foo/validate", Method.GET, AuthzRole.CREATOR,
                         AuthzRole.ADMIN, AuthzRole.MANAGER));

    list.add(new RestApi("/rest/v1/definitions", Method.GET, AuthzRole.ALL_ROLES));
    list.add(new RestApi("/rest/v1/definitions/stage/icons", Method.GET, AuthzRole.ALL_ROLES));


    list.add(new RestApi("/rest/v1/log", Method.GET, AuthzRole.ADMIN, AuthzRole.CREATOR, AuthzRole.MANAGER));
    list.add(new RestApi("/rest/v1/log/files", Method.GET, AuthzRole.ADMIN, AuthzRole.CREATOR, AuthzRole.MANAGER));
    list.add(new RestApi("/rest/v1/log/files/foo", Method.GET, AuthzRole.ADMIN, AuthzRole.CREATOR, AuthzRole.MANAGER));

    return list;
  }

  @Test
  public void testAuthorizationEnabled() throws Exception {
    test(createRestApis(), true);
  }

  @Test
  public void testAuthorizationDisabled() throws Exception {
    test(createRestApis(), false);
  }


}
