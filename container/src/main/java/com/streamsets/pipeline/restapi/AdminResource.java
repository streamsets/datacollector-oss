/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.LogStreamer;
import com.streamsets.pipeline.util.PipelineException;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/v1/admin")
public class AdminResource implements LogStreamer.Releaser {
  private static final String SHUTDOWN_SECRET_KEY = "http.shutdown.secret";
  private static final String SHUTDOWN_SECRET_DEFAULT = "secret";

  private static final String MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY = "max.logtail.concurrent.requests";
  private static final int MAX_LOGTAIL_CONCURRENT_REQUESTS_DEFAULT = 5;

  private final BuildInfo buildInfo;
  private final Configuration config;
  private final RuntimeInfo runtimeInfo;
  private static volatile int logTailClients;

  @Inject
  public AdminResource(Configuration configuration, RuntimeInfo runtimeInfo, BuildInfo buildInfo) {
    this.buildInfo = buildInfo;
    this.config = configuration;
    this.runtimeInfo = runtimeInfo;
  }

  @POST
  @Path("/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
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

  @POST
  @Path("/logout")
  public void logout(@Context HttpServletRequest request) throws PipelineStoreException {
    HttpSession session = request.getSession();
    session.invalidate();
  }


  @GET
  @Path("/log")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getLog() throws PipelineException, IOException {
    synchronized (AdminResource.class) {
      int maxClients = config.get(MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY, MAX_LOGTAIL_CONCURRENT_REQUESTS_DEFAULT);
      if (logTailClients < maxClients) {
        logTailClients++;
      } else {
        throw new PipelineException(ContainerError.CONTAINER_0300, maxClients);
      }
    }
    return Response.status(Response.Status.OK).entity(new LogStreamer(runtimeInfo, this).getLogTailReader()).build();
  }

  @GET
  @Path("/build-info")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getBuild() throws PipelineException, IOException {
    return Response.status(Response.Status.OK).entity(buildInfo).build();
  }

  @Override
  public void release() {
    synchronized (AdminResource.class) {
      logTailClients--;
    }
  }

}
