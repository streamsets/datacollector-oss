/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.store.PipelineStoreException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.eclipse.jetty.security.authentication.FormAuthenticator;

import javax.annotation.security.PermitAll;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import java.io.IOException;

@Path("/v1/authentication")
public class AuthenticationResource {


  @POST
  @Path("/login")
  @PermitAll
  public void login(@FormParam("username") String username,
                    @FormParam("password") String password,
                    @Context HttpServletRequest request,
                    @Context HttpServletResponse response) throws PipelineStoreException, IOException {

    HttpSession session = request.getSession();
    String redirectURL = (String)session.getAttribute(FormAuthenticator.__J_URI);
    if(redirectURL != null && redirectURL.contains("rest/v1/")) {
      session.setAttribute(FormAuthenticator.__J_URI, "/");
    }

    response.sendRedirect("j_security_check?j_username=" + username + "&j_password=" + password);
  }

  @POST
  @Path("/logout")
  @PermitAll
  public void logout(@Context HttpServletRequest request) throws PipelineStoreException {
    HttpSession session = request.getSession();
    session.invalidate();
  }

}
