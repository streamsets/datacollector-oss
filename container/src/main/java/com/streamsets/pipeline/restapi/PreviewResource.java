/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.RawSourcePreviewHelper;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.StageOutputJson;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.AuthzRole;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.preview.PreviewPipeline;
import com.streamsets.pipeline.runner.preview.PreviewPipelineBuilder;
import com.streamsets.pipeline.runner.preview.PreviewPipelineOutput;
import com.streamsets.pipeline.runner.preview.PreviewPipelineRunner;
import com.streamsets.pipeline.runner.preview.PreviewSourceOffsetTracker;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.ContainerError;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Path("/v1/pipeline-library")
@DenyAll
public class PreviewResource {
  private static final String MAX_BATCH_SIZE_KEY = "preview.maxBatchSize";
  private static final int MAX_BATCH_SIZE_DEFAULT = 10;
  private static final String MAX_BATCHES_KEY = "preview.maxBatches";
  private static final int MAX_BATCHES_DEFAULT = 10;

  //preview.maxBatchSize
  private final Configuration configuration;
  private final PipelineStoreTask store;
  private final StageLibraryTask stageLibrary;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public PreviewResource(Configuration configuration, Principal user, StageLibraryTask stageLibrary,
                         PipelineStoreTask store, RuntimeInfo runtimeInfo) {
    this.configuration = configuration;
    this.stageLibrary = stageLibrary;
    this.store = store;
    this.runtimeInfo = runtimeInfo;
  }

  @Path("/{name}/preview")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response preview(
    @PathParam("name") String name,
    @QueryParam("rev") String rev,
    @QueryParam("sourceOffset") String sourceOffset,
    @QueryParam("batchSize") @DefaultValue("" + Integer.MAX_VALUE) int batchSize,
    @QueryParam("batches") @DefaultValue("1") int batches,
    @QueryParam("skipTargets") @DefaultValue("true") boolean skipTargets)
    throws PipelineStoreException, PipelineRuntimeException, StageException {
    return previewWithOverride(name, rev, sourceOffset, batchSize, batches, skipTargets, null, Collections.EMPTY_LIST);
  }

  @Path("/{name}/preview")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response previewWithOverride(
      @PathParam("name") String name,
      @QueryParam("rev") String rev,
      @QueryParam("sourceOffset") String sourceOffset,
      @QueryParam("batchSize") @DefaultValue("" + Integer.MAX_VALUE) int batchSize,
      @QueryParam("batches") @DefaultValue("1") int batches,
      @QueryParam("skipTargets") @DefaultValue("true") boolean skipTargets,
      @QueryParam("endStage") String endStageInstanceName,
      List<StageOutputJson> stageOutputsToOverrideJson)
      throws PipelineStoreException, PipelineRuntimeException, StageException {

    Utils.checkState(runtimeInfo.getExecutionMode() != RuntimeInfo.ExecutionMode.SLAVE,
      "This operation is not supported in SLAVE mode");

    int maxBatchSize = configuration.get(MAX_BATCH_SIZE_KEY, MAX_BATCH_SIZE_DEFAULT);
    batchSize = Math.min(maxBatchSize, batchSize);
    int maxBatches = configuration.get(MAX_BATCHES_KEY, MAX_BATCHES_DEFAULT);
    batches = Math.min(maxBatches, batches);
    PipelineConfiguration pipelineConf = store.load(name, rev);
    SourceOffsetTracker tracker = new PreviewSourceOffsetTracker(sourceOffset);
    PreviewPipelineRunner runner = new PreviewPipelineRunner(tracker, batchSize, batches, skipTargets);
    try {
      PreviewPipeline pipeline = new PreviewPipelineBuilder(stageLibrary, name, pipelineConf,
        endStageInstanceName).build(runner);
      PreviewPipelineOutput previewOutput = pipeline.run(BeanHelper.unwrapStageOutput(stageOutputsToOverrideJson));
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapPreviewPipelineOutput(previewOutput))
        .build();
    } catch (PipelineRuntimeException ex) {
      if (ex.getErrorCode() == ContainerError.CONTAINER_0165) {
        return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(
          BeanHelper.wrapIssues(ex.getIssues())).build();
      } else {
        throw ex;
      }
    }
  }

  @Path("/{name}/rawSourcePreview")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response rawSourcePreview(
      @PathParam("name") String name,
      @QueryParam("rev") String rev,
      @Context UriInfo uriInfo) throws PipelineStoreException,
      PipelineRuntimeException, IOException {
    MultivaluedMap<String, String> previewParams = uriInfo.getQueryParameters();
    Map<String, String> preview = RawSourcePreviewHelper.preview(name, rev, previewParams, store, stageLibrary,
      configuration);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(preview).build();
  }

  @Path("/{name}/validateConfigs")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response validateConfigs(
      @PathParam("name") String name,
      @QueryParam("rev") String rev)
      throws PipelineStoreException, PipelineRuntimeException, StageException {
    PipelineConfiguration pipelineConf = store.load(name, rev);
    SourceOffsetTracker tracker = new PreviewSourceOffsetTracker("");
    PreviewPipelineRunner runner = new PreviewPipelineRunner(tracker, 10, 1, true);
    try {
      PreviewPipeline pipeline = new PreviewPipelineBuilder(stageLibrary, name, pipelineConf, null).build(runner);
      return Response.ok().type(MediaType.APPLICATION_JSON)
                     .entity(BeanHelper.wrapStageIssues(pipeline.validateConfigs())).build();
    } catch (PipelineRuntimeException ex) {
      if (ex.getErrorCode() == ContainerError.CONTAINER_0165) {
        return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(
            BeanHelper.wrapIssues(ex.getIssues())).build();
      } else {
        throw ex;
      }
    }
  }

}
