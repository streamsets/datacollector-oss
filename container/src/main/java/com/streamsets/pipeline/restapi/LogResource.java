/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.log.LogStreamer;
import com.streamsets.pipeline.log.LogUtils;
import com.streamsets.pipeline.main.RuntimeInfo;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/v1/log")
@DenyAll
public class LogResource {
  public static final String X_SDC_LOG_PREVIOUS_OFFSET_HEADER = "X-SDC-LOG-PREVIOUS-OFFSET";
  private final String logFile;

  @Inject
  public LogResource(RuntimeInfo runtimeInfo) throws RuntimeException {
    try {
      logFile = LogUtils.getLogFile(runtimeInfo);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.CREATOR, AuthzRole.MANAGER})
  public Response currentLog(@QueryParam("endingOffset") @DefaultValue("-1") long offset) throws IOException {
    final LogStreamer streamer = new LogStreamer(logFile, offset, 50 * 1024);
    StreamingOutput stream = new StreamingOutput() {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException {
        try {
          streamer.stream(output);
        } finally {
          streamer.close();
        }
      }
    };
    return Response.ok(stream).header(X_SDC_LOG_PREVIOUS_OFFSET_HEADER, streamer.getNewEndingOffset()).build();
  }

  private File[] getLogFiles() throws IOException {
    File log = new File(logFile);
    File logDir = log.getParentFile();
    final String logName = log.getName();
    return logDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(logName);
      }
    });
  }

  @GET
  @Path("/files")
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.CREATOR, AuthzRole.MANAGER})
  @SuppressWarnings("unchecked")
  public Response listLogFiles() throws IOException {
    File[] logFiles = getLogFiles();
    List<Map> list = new ArrayList<>();
    for (File logFile : logFiles) {
      Map map = new HashMap();
      map.put("file", logFile.getName());
      map.put("lastModified", logFile.lastModified());
      list.add(map);
    }
    return Response.ok(list).build();
  }

  @GET
  @Path("/files/{logName}")
  @Produces(MediaType.TEXT_PLAIN)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.CREATOR, AuthzRole.MANAGER})
  public Response getLogFile(@PathParam("logName") String logName,
                             @QueryParam("attachment") @DefaultValue("false") Boolean attachment) throws IOException {
    Response response;
    File logFile = null;
    for (File file : getLogFiles()) {
      if (file.getName().equals(logName)) {
        logFile = file;
        break;
      }
    }
    if (logFile != null) {
      if(attachment) {
        return Response.ok().
          header("Content-Disposition", "attachment; filename=" + logName).entity(new FileInputStream(logFile)).build();
      } else {
        response = Response.ok(new FileInputStream(logFile)).build();
      }

    } else {
      response = Response.status(Response.Status.NOT_FOUND).build();
    }
    return response;
  }

}
