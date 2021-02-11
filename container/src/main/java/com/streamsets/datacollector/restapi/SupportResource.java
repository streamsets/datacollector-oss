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

import com.google.common.io.ByteStreams;
import com.streamsets.datacollector.bundles.content.BundleGeneratorUtils;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.lang.management.ThreadInfo;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Path("/v1/system")
@Api(value = "StreamSets Support Useful Endpoints")
@DenyAll
@RequiresCredentialsDeployed
public class SupportResource {
  private static final Logger LOG = LoggerFactory.getLogger(SupportResource.class);

  /**
   * We're reusing support bundle executor service since it already exists and serves similar
   * role (e.g. helping support generate something).
   */
  private final ExecutorService executor;

  @Inject
  public SupportResource() {
    // We intentional limit this to a single threaded executor
    this.executor = Executors.newSingleThreadExecutor();
  }

  @POST
  @Path("/gc")
  @ApiOperation(value = "Run System.gc()", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response gc() throws PipelineStoreException {
    LOG.info("Requested System.gc() call");
    System.gc();
    return Response.ok().build();
  }

  @GET
  @Path("/threads/dumps")
  @ApiOperation(value = "Generates thread dumps in zip files waiting N milliseconds between each thread dump.", authorizations = @Authorization(value = "basic"))
  @Produces("application/octet-stream")
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response threadDumps(
      @QueryParam("count") @DefaultValue("2") int count,
      @QueryParam("waitMillis") @DefaultValue("1000") long waitMillis
  ) throws IOException {
    PipedInputStream inputStream = new PipedInputStream();
    PipedOutputStream outputStream = new PipedOutputStream();
    inputStream.connect(outputStream);
    ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream);

    // Generate the thread dumps
    executor.submit(() -> {
      try {
        for(int iteration = 0; iteration < count; iteration++) {
          if(iteration != 0) {
            Thread.sleep(waitMillis);
          }

          // Create new thread dump
          zipOutputStream.putNextEntry(new ZipEntry("threads_" + LocalDateTime.now().toString().replace(":", "_") + ".txt"));
          ThreadInfo[] threads = BundleGeneratorUtils.getThreadInfos();
          zipOutputStream.write(BundleGeneratorUtils.threadInfosToString(threads).getBytes(StandardCharsets.UTF_8));
          zipOutputStream.closeEntry();
          zipOutputStream.flush();
        }

        // And we're done
        zipOutputStream.close();
      } catch (Exception e) {
        LOG.error("Can't generate thread dumps", e);
      }

    } );

    // We explicitly use StreamingOutput as that forces the Java WS to "stream" output and not wait for the whole
    // zip file to be generated first.
    StreamingOutput streamingOutput = new StreamingOutput() {
      @Override
      public void write(OutputStream outputStream) throws IOException, WebApplicationException {
        ByteStreams.copy(inputStream, outputStream);
      }
    };

    // And asynchronously return the response
    return Response
      .ok()
      .header("content-disposition", "attachment; filename=\"thread_dumps_" + LocalDateTime.now() + ".zip\"")
      .entity(streamingOutput)
      .build();
  }
}
