/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.Configuration;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/admin")
public class AdminResource {
  private static final String SHUTDOWN_SECRET_KEY = "shutdown.secret";
  private static final String SHUTDOWN_SECRET_DEFAULT = "secret";

  private final Configuration config;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public AdminResource(Configuration configuration, RuntimeInfo runtimeInfo) {
    this.config = configuration;
    this.runtimeInfo = runtimeInfo;
  }

  @POST
  @Path("/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  public Response shutdown(@QueryParam("secret") String requestPassword) throws PipelineStoreException {
    Response response;
    String password = config.get(SHUTDOWN_SECRET_KEY, SHUTDOWN_SECRET_DEFAULT);
    if (password.equals(requestPassword)) {
      Thread thread = new Thread("Shutdown Request") {
        @Override
        public void run() {
          try {
            Thread.sleep(500);
          } catch (InterruptedException ex) {
          }
          runtimeInfo.shutdown();
        }
      };
      thread.setDaemon(true);
      thread.start();
      response = Response.ok().build();
    } else {
      response = Response.status(Response.Status.FORBIDDEN).build();
    }
    return response;
  }

}
