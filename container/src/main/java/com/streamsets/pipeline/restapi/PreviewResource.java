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
import com.streamsets.pipeline.restapi.bean.IssueJson;
import com.streamsets.pipeline.restapi.bean.PreviewPipelineOutputJson;
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
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;

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
@Api(value = "pipeline-library")
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

  @Path("/{pipelineName}/preview")
  @GET
  @ApiOperation(value = "Run Pipeline preview and get preview data", response = PreviewPipelineOutputJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response preview(
    @PathParam("pipelineName") String pipelineName,
    @QueryParam("rev") String rev,
    @QueryParam("batchSize") @DefaultValue("" + Integer.MAX_VALUE) int batchSize,
    @QueryParam("batches") @DefaultValue("1") int batches,
    @QueryParam("skipTargets") @DefaultValue("true") boolean skipTargets)
    throws PipelineStoreException, PipelineRuntimeException, StageException {
    return previewWithOverride(pipelineName, rev, batchSize, batches, skipTargets, null, Collections.EMPTY_LIST);
  }

  @Path("/{pipelineName}/preview")
  @POST
  @ApiOperation(value = "Run Pipeline preview by overriding passed stage instance data and get preview data",
    response = PreviewPipelineOutputJson.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response previewWithOverride(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") String rev,
      @QueryParam("batchSize") @DefaultValue("" + Integer.MAX_VALUE) int batchSize,
      @QueryParam("batches") @DefaultValue("1") int batches,
      @QueryParam("skipTargets") @DefaultValue("true") boolean skipTargets,
      @QueryParam("endStage") String endStageInstanceName,
      @ApiParam(name="stageOutputsToOverrideJson", required = true)  List<StageOutputJson> stageOutputsToOverrideJson)
      throws PipelineStoreException, PipelineRuntimeException, StageException {

    Utils.checkState(runtimeInfo.getExecutionMode() != RuntimeInfo.ExecutionMode.SLAVE,
      "This operation is not supported in SLAVE mode");

    int maxBatchSize = configuration.get(MAX_BATCH_SIZE_KEY, MAX_BATCH_SIZE_DEFAULT);
    batchSize = Math.min(maxBatchSize, batchSize);
    int maxBatches = configuration.get(MAX_BATCHES_KEY, MAX_BATCHES_DEFAULT);
    batches = Math.min(maxBatches, batches);
    PipelineConfiguration pipelineConf = store.load(pipelineName, rev);
    SourceOffsetTracker tracker = new PreviewSourceOffsetTracker(null);
    PreviewPipelineRunner runner = new PreviewPipelineRunner(runtimeInfo, tracker, batchSize, batches, skipTargets);
    try {
      PreviewPipeline pipeline = new PreviewPipelineBuilder(stageLibrary, pipelineName, pipelineConf,
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

  @Path("/{pipelineName}/rawSourcePreview")
  @GET
  @ApiOperation(value = "Get raw source preview data for pipeline name and revision", response = Map.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response rawSourcePreview(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") String rev,
      @Context UriInfo uriInfo) throws PipelineStoreException,
      PipelineRuntimeException, IOException {
    MultivaluedMap<String, String> previewParams = uriInfo.getQueryParameters();
    Map<String, String> preview = RawSourcePreviewHelper.preview(pipelineName, rev, previewParams, store, stageLibrary,
      configuration);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(preview).build();
  }

  @Path("/{pipelineName}/validateConfigs")
  @GET
  @ApiOperation(value = "Validate pipeline configuration and return validation status and issues",
    response = IssueJson.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response validateConfigs(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") String rev)
      throws PipelineStoreException, PipelineRuntimeException, StageException {
    PipelineConfiguration pipelineConf = store.load(pipelineName, rev);
    SourceOffsetTracker tracker = new PreviewSourceOffsetTracker("");
    PreviewPipelineRunner runner = new PreviewPipelineRunner(runtimeInfo, tracker, 10, 1, true);
    try {
      PreviewPipeline pipeline = new PreviewPipelineBuilder(stageLibrary, pipelineName, pipelineConf, null).build(runner);
      return Response.ok().type(MediaType.APPLICATION_JSON)
                     .entity(BeanHelper.wrapIssues(pipeline.validateConfigs())).build();
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
