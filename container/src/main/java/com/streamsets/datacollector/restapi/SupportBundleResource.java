/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Strings;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.support.StreamSetsSupportZendeskProvider;
import com.streamsets.support.SupportCredentials;
import com.streamsets.support.TicketPriority;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import org.apache.commons.io.FileUtils;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Path("/v1/system")
@Api(value = "system")
@DenyAll
public class SupportBundleResource extends BaseSDCRuntimeResource {

  private final StreamSetsSupportZendeskProvider zendeskProvider = new StreamSetsSupportZendeskProvider();

  @Inject
  public SupportBundleResource(BuildInfo buildInfo, RuntimeInfo runtimeInfo) {
    super(buildInfo, runtimeInfo);
    zendeskProvider.setBuildInfo(buildInfo);
  }

  @GET
  @Path("/support/createBundleZip")
  @ApiOperation(
      value = "Downloads support bundle zip file to client",
      authorizations = @Authorization(value = "basic"),
      produces = "application/zip"
  )
  @Produces(MediaType.TEXT_PLAIN)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.CREATOR,
      AuthzRole.MANAGER,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.MANAGER_REMOTE
  })
  public Response downloadSupportBundleZip() throws IOException {
    final StreamingOutput output = new StreamingOutput() {
      @Override
      public void write(OutputStream output) throws IOException {
        createSupportBundleZip(output);
      }
    };

    return Response
        .ok(output)
        .type("application/zip")
        .header("Content-Disposition", "attachment; filename=\"support_bundle.zip\"")
        .build();
  }

  @POST
  @Path("/support/submitBundleToZendesk")
  @ApiOperation(
      value = "Submits support bundle to Zendesk along with other specified parameters to create ticket",
      authorizations = @Authorization(value = "basic"),
      response = Map.class
  )
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.CREATOR,
      AuthzRole.MANAGER,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.MANAGER_REMOTE
  })
  public Response submitSupportBundleToZendesk(Map<String, String> params) throws IOException {
    final String username = params.get("zendeskUsername");
    final String password = params.get("zendeskPassword");
    final String token = params.get("zendeskToken");
    final String headline = params.get("headline");
    final String commentText = params.get("commentText");
    final TicketPriority priority = TicketPriority.valueOf(params.get("priority"));

    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    createSupportBundleZip(bos);

    SupportCredentials credentials;
    if (!Strings.isNullOrEmpty(token)) {
      credentials = new SupportCredentials(username, token, true);
    } else {
      credentials = new SupportCredentials(username, password);
    }

    final String ticketId =
        zendeskProvider.createNewSupportTicket(credentials, priority, headline, commentText, bos.toByteArray());

    final Map<String, String> responseMap = new HashMap<>();
    responseMap.put("ticketId", ticketId);

    return Response
        .ok(responseMap)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  private void createSupportBundleZip(OutputStream output) throws IOException {
    ZipOutputStream zos = new ZipOutputStream(output);

    for (final File logFile : determineAndGetAllLogFiles()) {
      zos.putNextEntry(new ZipEntry(logFile.getName()));
      FileUtils.copyFile(logFile, zos);
      zos.closeEntry();
    }
    zos.close();
  }

}
