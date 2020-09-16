/*
 * Copyright 2020 StreamSets Inc.
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

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.http.AsterContext;
import com.streamsets.datacollector.util.AuthzRole;
import io.swagger.annotations.Api;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/aregistration")
@Api(value = "Aster Registration")
@DenyAll
//@RequiresCredentialsDeployed
public class AsterRegistrationResource {

  private final AsterContext asterContext;

  @Inject
  public AsterRegistrationResource(AsterContext asterContext) {
    this.asterContext = asterContext;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE, AuthzRole.ADMIN_ACTIVATION})
  public Response initiate(@Context HttpServletRequest req, @Context HttpServletResponse res) {
    asterContext.handleRegistration(req, res);
    return null;
  }

  @POST
  @Consumes(MediaType.WILDCARD)
  @Produces(MediaType.TEXT_PLAIN)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE, AuthzRole.ADMIN_ACTIVATION})
  public Response complete(@Context HttpServletRequest req, @Context HttpServletResponse res, @FormParam("state") String state, @FormParam("code") String code) {
    req = new HttpServletRequestWrapper(req) {
      @Override
      public String getParameter(String name) {
        switch (name) {
          case "state":
            return state;
          case "code":
            return code;
          default:
            return super.getParameter(name);
        }
      }
    };
    asterContext.handleRegistration(req, res);
    return null;
  }

}
