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

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/authentication")
public class DisconnectedAuthenticationResource {
  private final AuthenticationResourceHandler handler;
  private final DisconnectedSSOService ssoService;

  @Inject
  public DisconnectedAuthenticationResource(AuthenticationResourceHandler handler, DisconnectedSSOService ssoService) {
    this.handler = handler;
    this.ssoService = ssoService;
  }

  @Path("/login")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response login(@Context HttpServletRequest httpRequest, LoginJson login) {
    if (ssoService.isEnabled()) {
      return handler.login(httpRequest, login);
    } else {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
  }

}
