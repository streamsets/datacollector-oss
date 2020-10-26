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

import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.Configuration;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

@Path("/v1/system")
@Api(value = "system")
@DenyAll
@RequiresCredentialsDeployed
public class ConfigurationResource {
  private static final String UI_PREFIX = "ui.";

  private final Configuration config;

  @Inject
  public ConfigurationResource(Configuration configuration) {
    this.config = configuration;
  }

  @GET
  @Path("/configuration/ui")
  @ApiOperation(value = "Returns UI SDC Configuration", response = Map.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getUIConfiguration() throws PipelineStoreException {
    Configuration configuration = config.getSubSetConfiguration(UI_PREFIX);
    configuration.set("ui.debug", String.valueOf(new File("/.sdc.debug").exists()));
    boolean inDaylight = TimeZone.getDefault().inDaylightTime(new Date());
    configuration.set("ui.server.timezone", TimeZone.getDefault().getDisplayName(inDaylight, TimeZone.SHORT));

    return Response.ok().type(MediaType.APPLICATION_JSON)
        .entity(configuration).build();
  }

  @GET
  @Path("configuration")
  @ApiOperation(value = "Returns ALL SDC Configuration", response = Map.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getConfiguration() throws PipelineStoreException {

    return Response.ok().type(MediaType.APPLICATION_JSON).entity(config.maskSensitiveConfigs().getUnresolvedConfiguration()).build();
  }
}
