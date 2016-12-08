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

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.support.StreamSetsSupportZendeskProvider;
import com.streamsets.support.SupportCredentials;
import com.streamsets.support.TicketPriority;
import com.sun.management.UnixOperatingSystemMXBean;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Path("/v1/system")
@Api(value = "system")
@DenyAll
public class SupportBundleResource extends BaseSDCRuntimeResource {

  private final StreamSetsSupportZendeskProvider zendeskProvider = new StreamSetsSupportZendeskProvider();

  private final RuntimeInfo runtimeInfo;

  @Inject
  public SupportBundleResource(BuildInfo buildInfo, RuntimeInfo runtimeInfo) {
    super(buildInfo, runtimeInfo);
    this.runtimeInfo = runtimeInfo;
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
    responseMap.put("ticketUrl", zendeskProvider.getPublicUrlForSupportTicket(ticketId));

    return Response
        .ok(responseMap)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  private void createSupportBundleZip(OutputStream output) throws IOException {
    ZipOutputStream zos = new ZipOutputStream(output);
    addLogsToBundle(zos);
    addRuntimeInfoToBundle(zos);
    addDataDirToBundle(zos);
    zos.close();
  }

  private void addLogsToBundle(ZipOutputStream zos) throws IOException {
    for (final File logFile : determineAndGetAllLogFiles()) {
      zos.putNextEntry(new ZipEntry("logs/"+logFile.getName()));
      FileUtils.copyFile(logFile, zos);
      zos.closeEntry();
    }
  }

  private void addRuntimeInfoToBundle(ZipOutputStream zos) throws IOException {

    final StringBuilder data = new StringBuilder();

    final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    if (memoryMXBean != null) {
      final MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
      if (heapUsage != null) {
        data.append("heap usage:\n");
        data.append(heapUsage.toString());
        data.append("\n\n");
      }

      final MemoryUsage nonHeapUsage = memoryMXBean.getNonHeapMemoryUsage();
      if (nonHeapUsage != null) {
        data.append("non-heap usage:\n");
        data.append(nonHeapUsage.toString());
        data.append("\n\n");
      }

      data.append("object count pending finalization: ");
      data.append(memoryMXBean.getObjectPendingFinalizationCount());
      data.append("\n\n");
    }

    final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    if(os != null && os instanceof UnixOperatingSystemMXBean) {
      if (os instanceof UnixOperatingSystemMXBean) {
        data.append("number open file descriptors: ");
        data.append(((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount());
        data.append("\n\n");
      }
    }

    final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    if (runtimeMXBean != null) {
      data.append("classpath:\n");
      data.append(runtimeMXBean.getClassPath());
      data.append("\n\n");
      data.append("boot classpath:\n");
      data.append(runtimeMXBean.getBootClassPath());
      data.append("\n\n");
      data.append("name:\n");
      data.append(runtimeMXBean.getName());
      data.append("\n\n");
      data.append("VM name:\n");
      data.append(runtimeMXBean.getVmName());
      data.append("\n\n");
      data.append("VM version:\n");
      data.append(runtimeMXBean.getVmVersion());
      data.append("\n\n");
      data.append("input arguments:\n");
      if (runtimeMXBean.getInputArguments() != null) {
        for (final String inputArg : runtimeMXBean.getInputArguments()) {
          data.append(inputArg);
          data.append(" ");
        }
      }
      data.append("\n\n");
      data.append("system properties:\n");
      if (runtimeMXBean.getSystemProperties() != null) {
        for (Map.Entry<String, String> sysProp : runtimeMXBean.getSystemProperties().entrySet()) {
          data.append(sysProp.getKey());
          data.append("=");
          data.append(sysProp.getValue());
          data.append("\n");
        }
      }
      data.append("\n\n");
      data.append("\n\n");
    }

    zos.putNextEntry(new ZipEntry("runtime_info.txt"));

    ObjectMapperFactory.get().writeValueAsString(data).getBytes(Charsets.UTF_8);

    zos.write(data.toString().getBytes());
    zos.closeEntry();
  }

  private void addDataDirToBundle(ZipOutputStream zos) throws IOException {
    final File dataDir = new File(runtimeInfo.getDataDir());
    addAllFilesToZip(dataDir, "data", zos);
  }

  private void addAllFilesToZip(File directory, String dirPathInZip, ZipOutputStream out) throws IOException {
    for (final File dataFile : directory.listFiles()) {
      final String newPath = dirPathInZip+"/"+dataFile.getName();
      if (dataFile.isDirectory()) {
        addAllFilesToZip(dataFile, newPath, out);
      } else {
        out.putNextEntry(new ZipEntry(newPath));
        FileUtils.copyFile(dataFile, out);
        out.closeEntry();
      }
    }
  }
}
