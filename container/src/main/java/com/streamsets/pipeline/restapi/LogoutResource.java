/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.store.PipelineStoreException;

import javax.annotation.security.PermitAll;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;

@Path("/v1/logout")
public class LogoutResource {

  @POST
  @PermitAll
  public void logout(@Context HttpServletRequest request) throws PipelineStoreException {
    HttpSession session = request.getSession();
    session.invalidate();
  }

}
