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

import com.google.common.io.Resources;
import com.streamsets.datacollector.event.client.impl.MovedDpmJerseyClientFilter;
import com.streamsets.datacollector.log.LogStreamer;
import com.streamsets.datacollector.log.LogUtils;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.AclPipelineStoreTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.util.Grok;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Path("/v1/system")
@Api(value = "system")
@DenyAll
@RequiresCredentialsDeployed
public class LogResource {
  private static final String X_SDC_LOG_PREVIOUS_OFFSET_HEADER = "X-SDC-LOG-PREVIOUS-OFFSET";
  private static final String EXCEPTION = "exception";
  private static long MAX_EXCEPTION = 10 * 1024; // 10 KB
  private final RuntimeInfo runtimeInfo;
  private final PipelineStoreTask store;
  private final Configuration config;

  @Inject
  public LogResource(
      RuntimeInfo runtimeInfo,
      Principal principal,
      PipelineStoreTask store,
      AclStoreTask aclStore,
      UserGroupManager userGroupManager,
      Configuration config
  ) {
    this.runtimeInfo = runtimeInfo;
    this.config = config;

    UserJson currentUser;
    if (runtimeInfo.isDPMEnabled()) {
      currentUser = new UserJson((SSOPrincipal)principal);
    } else {
      currentUser = userGroupManager.getUser(principal);
    }

    if (runtimeInfo.isAclEnabled()) {
      this.store = new AclPipelineStoreTask(store, aclStore, currentUser);
    } else {
      this.store = store;
    }
  }

  @GET
  @Path("/logs")
  @ApiOperation(value= "Return latest log file contents")
  @Produces(MediaType.TEXT_PLAIN)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.CREATOR,
      AuthzRole.MANAGER,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.MANAGER_REMOTE
  })
  public Response currentLog(
      @QueryParam("endingOffset") @DefaultValue("-1") long startOffset,
      @QueryParam("extraMessage") String extraMessage,
      @QueryParam("pipeline") String pipeline,
      @QueryParam("severity") String severity,
      @Context SecurityContext context,
      @Context HttpServletRequest request
  ) throws IOException, DataParserException, PipelineException {

    // Required for showing logs per pipeline in Pipeline page
    if (!context.isUserInRole(AuthzRole.ADMIN) && !context.isUserInRole(AuthzRole.ADMIN_REMOTE) &&
        runtimeInfo.isAclEnabled() ) {
      Utils.checkNotNull(pipeline, "Pipeline name");
    }

    if (!StringUtils.isEmpty(pipeline)) {
      // Validates Pipeline ACL Permission

      try {
        PipelineInfo pipelineInfo = store.getInfo(pipeline);
        pipeline = pipelineInfo.getTitle() + "/" + pipelineInfo.getPipelineId();
      } catch (PipelineException e) {
        // To support viewing logs of pipeline controlled by Control Hub after job is stopped
        // check if job is accessible for the user (ACL check)
        String[] strArr = pipeline.split("__");
        if (strArr.length >= 3 && runtimeInfo.isDPMEnabled()) {
          String pipelineTitle = isJobAccessibleFromControlHub(
              request,
              strArr[strArr.length - 2] + ":" + strArr[strArr.length - 1]
          );
          if (pipelineTitle != null) {
            pipeline = pipelineTitle + "/" + pipeline;
          } else {
            throw e;
          }
        } else {
          throw e;
        }
      }
    }

    String logFile = LogUtils.getLogFile(runtimeInfo);

    List<Map<String, String>> logData = new ArrayList<>();
    long offset = startOffset;

    LogStreamer streamer = new LogStreamer(logFile, offset, 50 * 1024);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    streamer.stream(outputStream);

    if(extraMessage != null) {
      outputStream.write(extraMessage.getBytes(StandardCharsets.UTF_8));
    }

    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(outputStream.toByteArray()), StandardCharsets.UTF_8));

    fetchLogData(bufferedReader, logData, pipeline, severity);

    offset = streamer.getNewEndingOffset();
    streamer.close();

    if((severity != null || pipeline != null) && logData.size() < 50) {
      // For filtering try to fetch more log data until it we get at least 50 lines of log data or it reaches top
      while (offset != 0 && logData.size() < 50) {
        streamer = new LogStreamer(logFile, offset, 50 * 1024);
        outputStream = new ByteArrayOutputStream();
        streamer.stream(outputStream);

        // merge last message if it is part of new messages
        if (!logData.isEmpty() && logData.get(0).get("timestamp") == null && logData.get(0).get(EXCEPTION) != null) {
          outputStream.write(logData.get(0).get(EXCEPTION).getBytes(StandardCharsets.UTF_8));
          logData.remove(0);
        }

        bufferedReader = new BufferedReader(new InputStreamReader(
            new ByteArrayInputStream(outputStream.toByteArray())));

        List<Map<String, String>> tempLogData = new ArrayList<>();
        fetchLogData(bufferedReader, tempLogData, pipeline, severity);

        //Add newly fetched log data to the beginning of the list
        tempLogData.addAll(logData);
        logData = tempLogData;

        offset = streamer.getNewEndingOffset();
        streamer.close();
      }
    }

    return Response.ok()
      .type(MediaType.APPLICATION_JSON)
      .entity(logData)
      .header(X_SDC_LOG_PREVIOUS_OFFSET_HEADER, offset)
      .build();
  }

  @GET
  @Path("/logs/files")
  @ApiOperation(value = "Returns all available SDC Log files", response = Map.class, responseContainer = "List",
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.ADMIN_REMOTE
  })
  @SuppressWarnings("unchecked")
  public Response listLogFiles() throws IOException {
    File[] logFiles = LogUtils.getLogFiles(runtimeInfo);
    List<Map> list = new ArrayList<>();
    for (File file : logFiles) {
      Map map = new HashMap();
      map.put("file", file.getName());
      map.put("lastModified", file.lastModified());
      list.add(map);
    }
    return Response.ok(list).build();
  }

  @GET
  @Path("/logs/files/{logName}")
  @ApiOperation(value = "Returns SDC Log File Content", response = String.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.TEXT_PLAIN)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getLogFile(
      @PathParam("logName") String logName,
      @QueryParam("attachment") @DefaultValue("false") Boolean attachment
  ) throws IOException {
    Response response;
    File newLogFile = null;
    for (File file : LogUtils.getLogFiles(runtimeInfo)) {
      if (file.getName().equals(logName)) {
        newLogFile = file;
        break;
      }
    }
    if (newLogFile != null) {
      FileInputStream logStream = new FileInputStream(newLogFile);
      if(attachment) {
        return Response.ok().
            header("Content-Disposition", "attachment; filename=" + logName).entity(logStream).build();
      } else {
        response = Response.ok(logStream).build();
      }
    } else {
      response = Response.status(Response.Status.NOT_FOUND).build();
    }
    return response;
  }

  private void fetchLogData(
      BufferedReader bufferedReader,
      List<Map<String, String>> logData,
      String pipeline,
      String severity
  ) throws IOException, DataParserException {
    Grok logFileGrok = LogUtils.getLogGrok(runtimeInfo);
    String thisLine;
    boolean lastMessageFiltered = false;
    boolean beginningOfRead = true;
    while ((thisLine = bufferedReader.readLine()) != null) {
      Map<String, String> namedGroupToValuesMap = logFileGrok.extractNamedGroups(thisLine);
      if(namedGroupToValuesMap != null) {
        beginningOfRead = false;
        if(severity != null && !severity.equals(namedGroupToValuesMap.get("severity"))) {
          lastMessageFiltered = true;
          continue;
        }

        if(pipeline != null && !pipeline.equals(namedGroupToValuesMap.get("s-entity"))) {
          lastMessageFiltered = true;
          continue;
        }

        lastMessageFiltered = false;
        logData.add(namedGroupToValuesMap);
      } else if(!lastMessageFiltered) {
        if(!logData.isEmpty()) {
          Map<String, String> lastLogData = logData.get(logData.size() - 1);

          if(lastLogData.containsKey(EXCEPTION)) {
            String exception = lastLogData.get(EXCEPTION);
            if(exception.length() <= MAX_EXCEPTION) {
              String newException;
              if(exception.length() + thisLine.length() > MAX_EXCEPTION) {
                newException = exception + "\n ... Truncated ...";
              } else {
                newException = exception + "\n" + thisLine;
              }
              lastLogData.put(EXCEPTION, newException);
            }
          } else {
            lastLogData.put(EXCEPTION, thisLine);
          }
        } else {
          if(beginningOfRead) {
            // Skipping the initial partial lines as we're randomly seeking into the log file
          } else {
            //First incomplete line
            Map<String, String> lastLogData = new HashMap<>();
            lastLogData.put(EXCEPTION, thisLine);
            logData.add(lastLogData);
          }
        }
      }
    }
  }

  @GET
  @Path("/log/config")
  @Produces(MediaType.TEXT_PLAIN)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getLogConfig(
      @QueryParam("default") @DefaultValue("false") boolean defaultConfig
  ) throws IOException {
    String fileName = runtimeInfo.getLog4jPropertiesFileName();
    InputStream log4jProperties;

    if (defaultConfig) {
      log4jProperties = Resources.getResource(fileName + "-default").openStream();
    } else {
      File file = new File(runtimeInfo.getConfigDir(), fileName);
      log4jProperties = new FileInputStream(file);
    }
    return Response.ok(log4jProperties).build();
  }

  @POST
  @Path("/log/config")
  @Consumes(MediaType.TEXT_PLAIN)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.ADMIN_REMOTE
  })
  public Response setLogConfig(
      InputStream payload
  ) throws IOException {
    File file = new File(runtimeInfo.getConfigDir(), runtimeInfo.getLog4jPropertiesFileName());
    try (OutputStream os = new FileOutputStream(file)) {
      IOUtils.copy(payload, os);
      payload.close();
    }
    return Response.ok().build();
  }

  private String isJobAccessibleFromControlHub(HttpServletRequest request, String jobId) {
    String controlHubPipelineName = null;
    if (runtimeInfo.isDPMEnabled()) {
      DpmClientInfo dpmClientInfo = runtimeInfo.getAttribute(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY);

      // Get DPM user auth token from request cookie
      SSOPrincipal ssoPrincipal = (SSOPrincipal) request.getUserPrincipal();
      String userAuthToken = ssoPrincipal.getTokenStr();

      Response response = null;
      try {
        // no need to use the MovedDpmJerseyClientFilter filter as this is a call using the user session
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(new MovedDpmJerseyClientFilter(dpmClientInfo));
        clientConfig.register(new CsrfProtectionFilter("CSRF"));
        Client client = ClientBuilder.newClient(clientConfig);

        response = client
            .target(dpmClientInfo.getDpmBaseUrl() + "jobrunner/rest/v1/job/" + jobId)
            .register(new CsrfProtectionFilter("CSRF"))
            .request()
            .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
            .header(SSOConstants.X_REST_CALL, true)
            .get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
          Map<String, Object> job = response.readEntity(new GenericType<Map<String, Object>>() {
          });
          if (job != null && job.containsKey("pipelineName")) {
            controlHubPipelineName = (String) job.get("pipelineName");
          }
        }
      } finally {
        if (response != null) {
          response.close();
        }
      }
    }
    return controlHubPipelineName;
  }

}
