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
package com.streamsets.datacollector.restapi;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.bundles.SupportBundle;
import com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.DPMInfoJson;
import com.streamsets.datacollector.restapi.bean.MultiStatusResponseJson;
import com.streamsets.datacollector.restapi.bean.SupportBundleContentDefinitionJson;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Path("/v1/system")
@Api(value = "system")
@DenyAll
@RequiresCredentialsDeployed
public class AdminResource {
  private static final Logger LOG = LoggerFactory.getLogger(AdminResource.class);
  private static final String APP_TOKEN_FILE = "application-token.txt";
  private static final String APP_TOKEN_FILE_PROP_VAL = "@application-token.txt@";

  private final RuntimeInfo runtimeInfo;
  private final Configuration config;
  private final UserGroupManager userGroupManager;
  private final SupportBundleManager supportBundleManager;

  @Inject
  public AdminResource(
    RuntimeInfo runtimeInfo,
    Configuration config,
    UserGroupManager userGroupManager,
    SupportBundleManager supportBundleManager
  ) {
    this.runtimeInfo = runtimeInfo;
    this.config = config;
    this.userGroupManager = userGroupManager;
    this.supportBundleManager = supportBundleManager;
  }

  @POST
  @Path("/shutdown")
  @ApiOperation(value = "Shutdown SDC", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response shutdown() throws PipelineStoreException {
    Thread thread = new Thread("Shutdown Request") {
      @Override
      public void run() {
        // sleeping  500ms to allow the HTTP response to go back
        ThreadUtil.sleep(500);
        runtimeInfo.shutdown(0);
      }
    };
    thread.setDaemon(true);
    thread.start();
    return Response.ok().build();
  }

  @POST
  @Path("/restart")
  @ApiOperation(value = "Restart SDC", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response restart() throws PipelineStoreException {
    Thread thread = new Thread("Shutdown Request") {
      @Override
      public void run() {
        // sleeping  500ms to allow the HTTP response to go back
        ThreadUtil.sleep(500);
        runtimeInfo.shutdown(88);
      }
    };
    thread.setDaemon(true);
    thread.start();
    return Response.ok().build();
  }

  @POST
  @Path("/enableDPM")
  @ApiOperation(
      value = "Enables DPM by getting application token from DPM",
      authorizations = @Authorization(value = "basic")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response enableDPM(DPMInfoJson dpmInfo) throws IOException {
    Utils.checkNotNull(dpmInfo, "DPMInfo");

    String dpmBaseURL = dpmInfo.getBaseURL();
    if (dpmBaseURL.endsWith("/")) {
      dpmBaseURL = dpmBaseURL.substring(0, dpmBaseURL.length() - 1);
    }

    // Since we support enabling/Disabling DPM, first check if token already exists for the given DPM URL.
    // If token exists skip first 3 steps
    String currentDPMBaseURL = config.get(RemoteSSOService.DPM_BASE_URL_CONFIG, "");
    String currentAppAuthToken = config.get(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "").trim();
    if (!currentDPMBaseURL.equals(dpmBaseURL) ||  currentAppAuthToken.length() == 0) {
      // 1. Login to DPM to get user auth token
      Response response = null;
      try {
        Map<String, String> loginJson = new HashMap<>();
        loginJson.put("userName", dpmInfo.getUserID());
        loginJson.put("password", dpmInfo.getUserPassword());
        response = ClientBuilder.newClient()
            .target(dpmBaseURL + "/security/public-rest/v1/authentication/login")
            .register(new CsrfProtectionFilter("CSRF"))
            .request()
            .post(Entity.json(loginJson));
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
          throw new RuntimeException(Utils.format("DPM Login failed, status code '{}': {}",
              response.getStatus(),
              response.readEntity(String.class)
          ));
        }
      } finally {
        if (response != null) {
          response.close();
        }
      }

      String userAuthToken = response.getHeaderString(SSOConstants.X_USER_AUTH_TOKEN);
      String appAuthToken = null;

      // 2. Create Data Collector application token
      try {
        Map<String, Object> newComponentJson = new HashMap<>();
        newComponentJson.put("organization", dpmInfo.getOrganization());
        newComponentJson.put("componentType", "dc");
        newComponentJson.put("numberOfComponents", 1);
        newComponentJson.put("active", true);
        response = ClientBuilder.newClient()
            .target(dpmBaseURL + "/security/rest/v1/organization/" + dpmInfo.getOrganization() + "/components")
            .register(new CsrfProtectionFilter("CSRF"))
            .request()
            .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
            .put(Entity.json(newComponentJson));
        if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
          throw new RuntimeException(Utils.format("DPM Create Application Token failed, status code '{}': {}",
              response.getStatus(),
              response.readEntity(String.class)
          ));
        }

        List<Map<String, Object>> newComponent = response.readEntity(new GenericType<List<Map<String,Object>>>() {});
        if (newComponent.size() > 0) {
          appAuthToken = (String) newComponent.get(0).get("fullAuthToken");
        } else {
          throw new RuntimeException("DPM Create Application Token failed: No token data from DPM Server.");
        }

      } finally {
        if (response != null) {
          response.close();
        }
        // Logout from DPM
        try {
          response = ClientBuilder.newClient()
              .target(dpmBaseURL + "/security/_logout")
              .register(new CsrfProtectionFilter("CSRF"))
              .request()
              .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
              .cookie(SSOConstants.AUTHENTICATION_COOKIE_PREFIX + "LOGIN", userAuthToken)
              .get();
        } finally {
          if (response != null) {
            response.close();
          }
        }
      }

      // 3. Update App Token file
      DataStore dataStore = new DataStore(new File(runtimeInfo.getConfigDir(), APP_TOKEN_FILE));
      try (OutputStream os = dataStore.getOutputStream()) {
        IOUtils.write(appAuthToken, os);
        dataStore.commit(os);
      } finally {
        dataStore.release();
      }
    }

    // 4. Update dpm.properties file
    try {
      FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
          new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
              .configure(new Parameters().properties()
                  .setFileName(runtimeInfo.getConfigDir() + "/dpm.properties")
                  .setThrowExceptionOnMissing(true)
                  .setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
                  .setIncludesAllowed(false));
      PropertiesConfiguration config = null;
      config = builder.getConfiguration();

      config.setProperty(RemoteSSOService.DPM_ENABLED, "true");
      config.setProperty(RemoteSSOService.DPM_BASE_URL_CONFIG, dpmBaseURL);
      config.setProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, APP_TOKEN_FILE_PROP_VAL);
      if (dpmInfo.getLabels() != null && dpmInfo.getLabels().size() > 0) {
        config.setProperty(RemoteEventHandlerTask.REMOTE_JOB_LABELS, StringUtils.join(dpmInfo.getLabels(), ','));
      } else {
        config.setProperty(RemoteEventHandlerTask.REMOTE_JOB_LABELS, "");
      }

      builder.save();
    } catch (ConfigurationException e) {
      throw new RuntimeException(Utils.format("Updating dpm.properties file failed: {}", e.getMessage()), e);
    }

    return Response.ok().build();
  }

  @POST
  @Path("/disableDPM")
  @ApiOperation(
      value = "Disables DPM",
      authorizations = @Authorization(value = "basic")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response disableDPM(@Context HttpServletRequest request) throws IOException {
    // check if DPM enabled
    if (!runtimeInfo.isDPMEnabled()) {
      throw new RuntimeException("disableDPM is supported only when DPM is enabled");
    }

    String dpmBaseURL = config.get(RemoteSSOService.DPM_BASE_URL_CONFIG, "");
    if (dpmBaseURL.endsWith("/")) {
      dpmBaseURL = dpmBaseURL.substring(0, dpmBaseURL.length() - 1);
    }

    // 1. Get DPM user auth token from request cookie
    SSOPrincipal ssoPrincipal = (SSOPrincipal)request.getUserPrincipal();
    String userAuthToken = ssoPrincipal.getTokenStr();
    String organizationId = ssoPrincipal.getOrganizationId();
    String componentId = runtimeInfo.getId();

    // 2. Deactivate Data Collector System Component
    Response response = null;
    try {
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/security/rest/v1/organization/" + organizationId + "/components/deactivate")
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
          .header(SSOConstants.X_REST_CALL, true)
          .post(Entity.json(ImmutableList.of(componentId)));
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format(
            " Deactivate Data Collector System Component from DPM failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }

    // 3. Delete Data Collector System Component
    try {
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/security/rest/v1/organization/" + organizationId + "/components/delete")
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
          .header(SSOConstants.X_REST_CALL, true)
          .post(Entity.json(ImmutableList.of(componentId)));
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format(
            " Deactivate Data Collector System Component from DPM failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }

    // 4. Delete from Job Runner SDC list
    try {
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/jobrunner/rest/v1/sdc/" + componentId)
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
          .header(SSOConstants.X_REST_CALL, true)
          .delete();
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format(
            "Delete from DPM Job Runner SDC list failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }

    // 5. Update App Token file
    DataStore dataStore = new DataStore(new File(runtimeInfo.getConfigDir(), APP_TOKEN_FILE));
    try (OutputStream os = dataStore.getOutputStream()) {
      IOUtils.write("", os);
      dataStore.commit(os);
    } finally {
      dataStore.release();
    }

    // 4. Update dpm.properties file
    try {
      FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
          new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
              .configure(new Parameters().properties()
                  .setFileName(runtimeInfo.getConfigDir() + "/dpm.properties")
                  .setThrowExceptionOnMissing(true)
                  .setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
                  .setIncludesAllowed(false));
      PropertiesConfiguration config = null;
      config = builder.getConfiguration();
      config.setProperty(RemoteSSOService.DPM_ENABLED, "false");
      builder.save();
    } catch (ConfigurationException e) {
      throw new RuntimeException(Utils.format("Updating dpm.properties file failed: {}", e.getMessage()), e);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/createDPMUsers")
  @ApiOperation(
      value = "Create Users & Groups in DPM",
      authorizations = @Authorization(value = "basic")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response createDPMUsers(DPMInfoJson dpmInfo) throws IOException {
    Utils.checkNotNull(dpmInfo, "DPMInfo");

    List<String> successEntities = new ArrayList<>();
    List<String> errorMessages = new ArrayList<>();

    String dpmBaseURL = dpmInfo.getBaseURL();
    if (dpmBaseURL.endsWith("/")) {
      dpmBaseURL = dpmBaseURL.substring(0, dpmBaseURL.length() - 1);
    }

    // 1. Login to DPM to get user auth token
    Response response = null;
    try {
      Map<String, String> loginJson = new HashMap<>();
      loginJson.put("userName", dpmInfo.getUserID());
      loginJson.put("password", dpmInfo.getUserPassword());
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/security/public-rest/v1/authentication/login")
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .post(Entity.json(loginJson));
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format("DPM Login failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }

    String userAuthToken = response.getHeaderString(SSOConstants.X_USER_AUTH_TOKEN);
    String appAuthToken = null;

    // 2. Create Groups
    for (Map<String, Object> group: dpmInfo.getDpmGroupList()) {
      try {
        response = ClientBuilder.newClient()
            .target(dpmBaseURL + "/security/rest/v1/organization/" + dpmInfo.getOrganization() + "/groups")
            .register(new CsrfProtectionFilter("CSRF"))
            .request()
            .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
            .put(Entity.json(group));

        if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
          errorMessages.add(Utils.format("DPM Create Group '{}' failed, status code '{}': {}",
              group.get("id"),
              response.getStatus(),
              response.readEntity(String.class)
          ));
        } else {
          successEntities.add(Utils.format("Created DPM Group '{}' successfully", group.get("id")));
        }
      } finally {
        if (response != null) {
          response.close();
        }
      }
    }

    // 3. Create Users
    for (Map<String, Object> user: dpmInfo.getDpmUserList()) {
      try {
        response = ClientBuilder.newClient()
            .target(dpmBaseURL + "/security/rest/v1/organization/" + dpmInfo.getOrganization() + "/users")
            .register(new CsrfProtectionFilter("CSRF"))
            .request()
            .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
            .put(Entity.json(user));

        if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
          errorMessages.add(Utils.format("DPM Create User '{}' failed, status code '{}': {}",
              user.get("id"),
              response.getStatus(),
              response.readEntity(String.class)
          ));
        } else {
          successEntities.add(Utils.format("Created DPM User '{}' successfully", user.get("id")));
        }
      } finally {
        if (response != null) {
          response.close();
        }
      }
    }

    // 4. Logout from DPM
    try {
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/security/_logout")
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
          .cookie(SSOConstants.AUTHENTICATION_COOKIE_PREFIX + "LOGIN", userAuthToken)
          .get();
    } finally {
      if (response != null) {
        response.close();
      }
    }

    return Response.status(207)
        .type(MediaType.APPLICATION_JSON)
        .entity(new MultiStatusResponseJson<>(successEntities, errorMessages)).build();
  }

  @GET
  @Path("/threads")
  @ApiOperation(value = "Returns Thread Dump along with stack trace", response = Map.class, responseContainer = "List",
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response getThreadsDump() throws IOException {
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threads = threadMXBean.dumpAllThreads(true, true);
    List<Map> augmented = new ArrayList<>(threads.length);
    for (ThreadInfo thread : threads) {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("threadInfo", thread);
      map.put("userTimeNanosecs", threadMXBean.getThreadUserTime(thread.getThreadId()));
      map.put("cpuTimeNanosecs", threadMXBean.getThreadCpuTime(thread.getThreadId()));
      augmented.add(map);
    }
    return Response.ok(augmented).build();
  }

  @GET
  @Path("/directories")
  @ApiOperation(value = "Returns SDC Directories", response = Map.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response getSDCDirectories() throws IOException {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("runtimeDir", runtimeInfo.getRuntimeDir());
    map.put("configDir", runtimeInfo.getConfigDir());
    map.put("dataDir", runtimeInfo.getDataDir());
    map.put("logDir", runtimeInfo.getLogDir());
    map.put("resourcesDir", runtimeInfo.getResourcesDir());
    map.put("libsExtraDir", runtimeInfo.getLibsExtraDir());
    return Response.ok(map).build();
  }

  @GET
  @Path("/users")
  @ApiOperation(
      value = "Returns All Users Info",
      response = UserJson.class,
      responseContainer = "List",
      authorizations = @Authorization(value = "basic")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.CREATOR,
      AuthzRole.CREATOR_REMOTE
  })
  public Response getUsers() throws IOException {
    return Response.ok(userGroupManager.getUsers()).build();
  }

  @GET
  @Path("/groups")
  @ApiOperation(
      value = "Returns All Group names",
      response = UserJson.class,
      responseContainer = "List",
      authorizations = @Authorization(value = "basic")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.CREATOR,
      AuthzRole.CREATOR_REMOTE
  })
  public Response getGroups() throws IOException {
    return Response.ok(userGroupManager.getGroups()).build();
  }

  @GET
  @Path("/bundle/list")
  @ApiOperation(
      value = "Return list of available content generators for support bundles.",
      response = SupportBundleContentDefinitionJson.class,
      responseContainer = "List",
      authorizations = @Authorization(value = "basic")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getSupportBundlesContentGenerators() throws IOException {
    return Response.ok(BeanHelper.wrapSupportBundleDefinitions(supportBundleManager.getContentDefinitions())).build();
  }

  @GET
  @Path("/bundle/generate")
  @ApiOperation(
      value = "Generates a new support bundle.",
      response = Object.class,
      authorizations = @Authorization(value = "basic")
  )
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces("application/octet-stream")
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.ADMIN_REMOTE
  })
  public Response createSupportBundlesContentGenerators(
    @QueryParam("generators") @DefaultValue("") String generators
  ) throws IOException {
    SupportBundle bundle = supportBundleManager.generateNewBundle(getGeneratorList(generators));

    return Response
      .ok()
      .header("content-disposition", "attachment; filename=\"" + bundle.getBundleName() + "\"")
      .entity(bundle.getInputStream())
      .build();
  }

  @GET
  @Path("/bundle/upload")
  @ApiOperation(
      value = "Generates new support bundle and uploads it to StreamSets.",
      response = Object.class,
      authorizations = @Authorization(value = "basic")
  )
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.ADMIN_REMOTE
  })
  public Response uploadSupportBundlesContentGenerators(
    @QueryParam("generators") @DefaultValue("") String generators
  ) throws IOException {
    // The call with throw IOException on any error that will be propagated to the client
    supportBundleManager.uploadNewBundle(getGeneratorList(generators));

    return Response
      .ok()
      .build();
  }

  private List<String> getGeneratorList(String queryValue) {
    if(StringUtils.isEmpty(queryValue)) {
      return Collections.emptyList();
    }

    return Arrays.asList(queryValue.split(","));
  }
}
