/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi;

import com.google.common.collect.ImmutableMap;
import com.streamsets.dc.execution.Manager;
import com.streamsets.dc.execution.PreviewOutput;
import com.streamsets.dc.execution.Previewer;
import com.streamsets.dc.execution.RawPreview;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.IssueJson;
import com.streamsets.pipeline.restapi.bean.PreviewPipelineOutputJson;
import com.streamsets.pipeline.restapi.bean.StageOutputJson;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.AuthzRole;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineException;
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

@Path("/v1")
@Api(value = "preview")
@DenyAll
public class PreviewResource {
  private static final String MAX_BATCH_SIZE_KEY = "preview.maxBatchSize";
  private static final int MAX_BATCH_SIZE_DEFAULT = 10;
  private static final String MAX_BATCHES_KEY = "preview.maxBatches";
  private static final int MAX_BATCHES_DEFAULT = 10;

  //preview.maxBatchSize
  private final Manager manager;
  private final Configuration configuration;
  private final RuntimeInfo runtimeInfo;
  private final String user;

  @Inject
  public PreviewResource(Manager manager, Configuration configuration, Principal principal, RuntimeInfo runtimeInfo) {
    this.manager = manager;
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
    this.user = principal.getName();
  }

  @Path("/preview/{pipelineName}/create")
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
      @QueryParam("timeout") @DefaultValue("2000") long timeout,
      @ApiParam(name="stageOutputsToOverrideJson", required = true)  List<StageOutputJson> stageOutputsToOverrideJson)
      throws PipelineException, StageException {

    if (stageOutputsToOverrideJson == null) {
      stageOutputsToOverrideJson = Collections.EMPTY_LIST;
    }
    Utils.checkState(runtimeInfo.getExecutionMode() != RuntimeInfo.ExecutionMode.SLAVE,
      "This operation is not supported in SLAVE mode");

    int maxBatchSize = configuration.get(MAX_BATCH_SIZE_KEY, MAX_BATCH_SIZE_DEFAULT);
    batchSize = Math.min(maxBatchSize, batchSize);
    int maxBatches = configuration.get(MAX_BATCHES_KEY, MAX_BATCHES_DEFAULT);
    batches = Math.min(maxBatches, batches);

    Previewer previewer = manager.createPreviewer(this.user, pipelineName, rev);

    try {
      previewer.start(batches, batchSize, skipTargets, endStageInstanceName,
        BeanHelper.unwrapStageOutput(stageOutputsToOverrideJson), timeout);

      return Response.ok().type(MediaType.APPLICATION_JSON).entity(ImmutableMap.of("previewerId", previewer.getId()))
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

  @Path("/preview-id/{previewerId}/status")
  @GET
  @ApiOperation(value = "Return Preview status by previewer ID", response = PreviewPipelineOutputJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response getPreviewStatus(@PathParam("previewerId") String previewerId)
    throws PipelineException, StageException {
    Previewer previewer = manager.getPreview(previewerId);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(ImmutableMap.of("status", previewer.getStatus())).build();
  }

  @Path("/preview-id/{previewerId}")
  @GET
  @ApiOperation(value = "Return Preview Data by previewer ID", response = PreviewPipelineOutputJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response getPreviewData(@PathParam("previewerId") String previewerId)
    throws PipelineException, StageException {
    Previewer previewer = manager.getPreview(previewerId);
    PreviewOutput previewOutput = previewer.getOutput();
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapPreviewOutput(previewOutput)).build();
  }

  @Path("/preview-id/{previewerId}/cancel")
  @POST
  @ApiOperation(value = "Stop Preview by previewer ID", response = PreviewPipelineOutputJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response stopPreview(@PathParam("previewerId") String previewerId)
    throws PipelineException, StageException {
    Previewer previewer = manager.getPreview(previewerId);
    previewer.stop();
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewer.getStatus()).build();
  }

  @Path("/preview/{pipelineName}/rawSourcePreview")
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
    Previewer previewer = manager.createPreviewer(this.user, pipelineName, rev);
    RawPreview rawPreview = previewer.getRawSource(4 * 1024, previewParams);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(rawPreview.getData()).build();
  }

  @Path("/pipeline/{pipelineName}/validate")
  @GET
  @ApiOperation(value = "Validate pipeline configuration and return validation status and issues",
    response = IssueJson.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response validateConfigs(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") String rev,
      @QueryParam("timeout") @DefaultValue("2000") long timeout)
      throws PipelineException, StageException {
    try {
      Previewer previewer = manager.createPreviewer(this.user, pipelineName, rev);
      previewer.validateConfigs(timeout);

      return Response.ok().type(MediaType.APPLICATION_JSON)
                     .entity(ImmutableMap.of("previewerId", previewer.getId())).build();
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
