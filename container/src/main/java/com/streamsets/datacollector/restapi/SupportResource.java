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

import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.Configuration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/system")
@Api(value = "StreamSets Support Useful Endpoints")
@DenyAll
@RequiresCredentialsDeployed
public class SupportResource {
  private static final Logger LOG = LoggerFactory.getLogger(SupportResource.class);

  private final RuntimeInfo runtimeInfo;
  private final Configuration config;
  private final UserGroupManager userGroupManager;
  private final SupportBundleManager supportBundleManager;

  @Inject
  public SupportResource(
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
  @Path("/gc")
  @ApiOperation(value = "Run System.gc()", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response gc() throws PipelineStoreException {
    LOG.info("Requested System.gc() call");
    System.gc();
    return Response.ok().build();
  }
}
