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

import com.google.api.client.util.Lists;
import com.google.common.base.Splitter;
import com.streamsets.datacollector.bundles.BundleType;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.bundles.SupportBundle;
import com.streamsets.datacollector.cli.sch.SchAdmin;
import com.streamsets.datacollector.inspector.HealthInspectorManager;
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
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
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
    LOG.info("Shutdown requested.");
    Thread thread = new Thread("Shutdown Request") {
      @Override
      public void run() {
        // sleeping  500ms to allow the HTTP response to go back
        ThreadUtil.sleep(500);
        LOG.info("Initiating shutdown");
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
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE, AuthzRole.ADMIN_ACTIVATION})
  public Response restart() throws PipelineStoreException {
    LOG.info("Restart requested.");
    Thread thread = new Thread("Shutdown Request") {
      @Override
      public void run() {
        // sleeping  500ms to allow the HTTP response to go back
        ThreadUtil.sleep(500);
        LOG.info("Initiating restart");
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
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE, AuthzRole.ADMIN_ACTIVATION})
  public Response enableDPM(DPMInfoJson dpmInfo) throws IOException {
    Utils.checkNotNull(dpmInfo, "DPMInfo");
    SchAdmin.enableDPM(dpmInfo, new SchAdmin.Context(runtimeInfo, config));
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

     // 1. Get DPM user auth token from request cookie
    SSOPrincipal ssoPrincipal = (SSOPrincipal)request.getUserPrincipal();
    String userAuthToken = ssoPrincipal.getTokenStr();
    String organizationId = ssoPrincipal.getOrganizationId();

    SchAdmin.disableDPM(userAuthToken, organizationId, new SchAdmin.Context(runtimeInfo, config));

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
  @Path("/health/categories")
  @ApiOperation(value = "Get list of available health check categories", response = Map.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response getHealthInspector() throws IOException {
    HealthInspectorManager healthInspector = new HealthInspectorManager(
        config,
        runtimeInfo
    );
    return Response.ok(healthInspector.availableInspectors()).build();
  }

  @GET
  @Path("/health/report")
  @ApiOperation(value = "Generate health report for given categories (all by default).", response = Map.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response getHealthInspector(
      @QueryParam("categories") @DefaultValue("") String categories
  ) throws IOException {
    HealthInspectorManager healthInspector = new HealthInspectorManager(
        config,
        runtimeInfo
    );
    return Response.ok(
        healthInspector.inspectHealth(Lists.newArrayList(Splitter.on(" , ").trimResults().omitEmptyStrings().split(categories)))
    ).build();
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
    SupportBundle bundle = supportBundleManager.generateNewBundle(getGeneratorList(generators), BundleType.SUPPORT);

    return Response
      .ok()
      .header("content-disposition", "attachment; filename=\"" + bundle.getBundleName() + "\"")
      .entity(bundle.getInputStream())
      .build();
  }

  private List<String> getGeneratorList(String queryValue) {
    if(StringUtils.isEmpty(queryValue)) {
      return Collections.emptyList();
    }

    return Arrays.asList(queryValue.split(","));
  }
}
