/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.prodmanager.*;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.SourceOffset;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import org.apache.commons.io.IOUtils;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Path("/v1/pipeline")
public class PipelineManagerResource {

  private final PipelineProductionManagerTask pipelineManager;

  @Inject
  public PipelineManagerResource(PipelineProductionManagerTask pipelineManager) {
    this.pipelineManager = pipelineManager;

  }

  @Path("/status")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatus() throws PipelineStateException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getPipelineState()).build();
  }

  @Path("/start")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response startPipeline(@QueryParam("rev") String rev) throws PipelineStoreException, PipelineRuntimeException
      , StageException
      , PipelineStateException {

    PipelineState ps = pipelineManager.startPipeline(rev);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        ps).build();
  }

  @Path("/stop")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response stopPipeline() throws PipelineStateException {

    PipelineState ps = pipelineManager.stopPipeline();
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        ps).build();
  }

  @Path("/offset")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  public Response setOffset(@QueryParam("name") String name, @QueryParam("rev") String rev
      , @QueryParam("offset") String offset) throws PipelineStateException {
    SourceOffset so = new SourceOffset(pipelineManager.setOffset(offset));
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        so).build();
  }

  @Path("/snapshot")
  @PUT
  public Response captureSnapshot(@QueryParam("batchSize") int batchSize) throws PipelineStateException {
    pipelineManager.captureSnapshot(batchSize);
    return Response.ok().build();
  }

  @Path("/snapshot")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSnapshot() {
    final InputStream snapshot = pipelineManager.getSnapshot();
    if(snapshot == null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(null).build();
    }
    StreamingOutput stream = new StreamingOutput() {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException {
        try {
          IOUtils.copy(snapshot, output);
        } catch (Exception e) {
          throw new WebApplicationException(e);
        }
      }
    };
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(stream).build();
  }

  @Path("/snapshot")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteSnapshot() {
    pipelineManager.deleteSnapshot();
    return Response.ok().build();
  }

  @Path("/snapshot")
  @HEAD
  @Produces(MediaType.APPLICATION_JSON)
  public Response snapshotStatus() {
    SnapshotStatus status = pipelineManager.snapshotStatus();
    return Response.ok().type(MediaType.APPLICATION_JSON).header("exists", status.isExists())
        .header("snapshotInProgress", status.isSnapshotInProgress()).build();
  }

  //TODO metrics

}
