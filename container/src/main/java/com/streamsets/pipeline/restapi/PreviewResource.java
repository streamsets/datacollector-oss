/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RawSourceDefinition;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.prodmanager.RawSourcePreviewHelper;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.preview.PreviewPipeline;
import com.streamsets.pipeline.runner.preview.PreviewPipelineBuilder;
import com.streamsets.pipeline.runner.preview.PreviewPipelineOutput;
import com.streamsets.pipeline.runner.preview.PreviewPipelineRunner;
import com.streamsets.pipeline.runner.preview.PreviewSourceOffsetTracker;
import com.streamsets.pipeline.runner.preview.PreviewStageRunner;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import org.apache.commons.io.IOUtils;
import com.streamsets.pipeline.util.ContainerError;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.ReaderInputStream;

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
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/v1/pipeline-library")
public class PreviewResource {
  private static final String MAX_BATCH_SIZE_KEY = "preview.maxBatchSize";
  private static final int MAX_BATCH_SIZE_DEFAULT = 10;
  private static final String MAX_BATCHES_KEY = "preview.maxBatches";
  private static final int MAX_BATCHES_DEFAULT = 10;

  //preview.maxBatchSize
  private final Configuration configuration;
  private final PipelineStoreTask store;
  private final StageLibraryTask stageLibrary;

  @Inject
  public PreviewResource(Configuration configuration, Principal user, StageLibraryTask stageLibrary, PipelineStoreTask store) {
    this.configuration = configuration;
    this.stageLibrary = stageLibrary;
    this.store = store;
  }

  @Path("/{name}/preview")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response preview(
      @PathParam("name") String name,
      @QueryParam("rev") String rev,
      @QueryParam("sourceOffset") String sourceOffset,
      @QueryParam("batchSize") @DefaultValue("" + Integer.MAX_VALUE) int batchSize,
      @QueryParam("batches") @DefaultValue("1") int batches,
      @QueryParam("skipTargets") @DefaultValue("true") boolean skipTargets)
      throws PipelineStoreException, PipelineRuntimeException, StageException {
    int maxBatchSize = configuration.get(MAX_BATCH_SIZE_KEY, MAX_BATCH_SIZE_DEFAULT);
    batchSize = Math.min(maxBatchSize, batchSize);
    int maxBatches = configuration.get(MAX_BATCHES_KEY, MAX_BATCHES_DEFAULT);
    batches = Math.min(maxBatches, batches);
    PipelineConfiguration pipelineConf = store.load(name, rev);
    SourceOffsetTracker tracker = new PreviewSourceOffsetTracker(sourceOffset);
    PreviewPipelineRunner runner = new PreviewPipelineRunner(tracker, batchSize, batches, skipTargets);
    PreviewPipeline pipeline = new PreviewPipelineBuilder(stageLibrary, name, pipelineConf).build(runner);
    PreviewPipelineOutput previewOutput = pipeline.run();
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewOutput).build();
  }

  @Path("/{name}/preview")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @SuppressWarnings("unchecked")
  public Response previewRunStage(
      @PathParam("name") String name,
      @QueryParam("rev") String rev,
      @QueryParam("stageInstance") String stageInstance, List<RecordImpl> records)
      throws PipelineStoreException, PipelineRuntimeException, StageException {
    Preconditions.checkNotNull(stageInstance, "stageInstance cannot be null");
    Preconditions.checkNotNull(records, "records (POST payload) cannot be null");
    PipelineConfiguration pipelineConf = store.load(name, rev);
    PreviewStageRunner runner = new PreviewStageRunner(stageInstance, (List<Record>) (List) records);
    PreviewPipeline pipeline = new PreviewPipelineBuilder(stageLibrary, name, pipelineConf).build(runner);
    PreviewPipelineOutput previewOutput = pipeline.run();
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewOutput).build();
  }

  @Path("/{name}/rawSourcePreview")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
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

}
