/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.store.PipelineStoreException;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/admin")
@DenyAll
public class AdminResource {

  private final RuntimeInfo runtimeInfo;

  @Inject
  public AdminResource(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  @POST
  @Path("/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed(AuthzRole.ADMIN)
  public Response shutdown() throws PipelineStoreException {
    Thread thread = new Thread("Shutdown Request") {
      @Override
      public void run() {
        try {
          Thread.sleep(500);
        } catch (InterruptedException ex) {
          //NOP
        }
        runtimeInfo.shutdown();
      }
    };
    thread.setDaemon(true);
    thread.start();
    return Response.ok().build();
  }

}
