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

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RawSourceDefinition;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
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
import com.streamsets.pipeline.util.ContainerErrors;
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

import java.io.Reader;
import java.lang.reflect.Field;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

@Path("/v1/pipeline-library")
public class PreviewResource {
  private static final String MAX_BATCH_SIZE_KEY = "preview.maxBatchSize";
  private static final int MAX_BATCH_SIZE_DEFAULT = 10;
  private static final String MAX_BATCHES_KEY = "preview.maxBatches";
  private static final int MAX_BATCHES_DEFAULT = 10;
  private static final String MAX_SOURCE_PREVIEW_SIZE_KEY = "preview.maxSourcePreviewSize";
  private static final int MAX_SOURCE_PREVIEW_SIZE_DEFAULT = 4*1024;

  //preview.maxBatchSize
  private final Configuration configuration;
  private final PipelineStoreTask store;
  private final StageLibraryTask stageLibrary;
  private final String user;


  @Inject
  public PreviewResource(Configuration configuration, Principal user, StageLibraryTask stageLibrary, PipelineStoreTask store) {
    this.configuration = configuration;
    this.user = user.getName();
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
      PipelineRuntimeException {

    MultivaluedMap<String, String> previewParams = uriInfo.getQueryParameters();

    PipelineConfiguration pipelineConf = store.load(name, rev);
    if(pipelineConf.getStages().isEmpty()) {
      throw new PipelineRuntimeException(ContainerErrors.CONTAINER_0159, name);
    }

    //Source stage is always the first one in the entire pipeline
    StageConfiguration stageConf = pipelineConf.getStages().get(0);
    StageDefinition stageDefinition = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
        stageConf.getStageVersion());
    RawSourceDefinition rawSourceDefinition = stageDefinition.getRawSourceDefinition();
    List<ConfigDefinition> configDefinitions = rawSourceDefinition.getConfigDefinitions();

    validateParameters(previewParams, configDefinitions);

    //Attempt to load the previewer class from stage class loader
    Class previewerClass = null;
    try {
      previewerClass = stageDefinition.getStageClassLoader().loadClass(stageDefinition.getRawSourceDefinition()
          .getRawSourcePreviewerClass());
    } catch (ClassNotFoundException e) {
      //Try loading from this class loader
      try {
        previewerClass = getClass().getClassLoader().loadClass(stageDefinition.getRawSourceDefinition()
            .getRawSourcePreviewerClass());
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException(e1);
      }
    }

    int bytesToRead = configuration.get(MAX_SOURCE_PREVIEW_SIZE_KEY, MAX_SOURCE_PREVIEW_SIZE_DEFAULT);
    bytesToRead = Math.min(bytesToRead, MAX_SOURCE_PREVIEW_SIZE_DEFAULT);

    Reader reader;
    RawSourcePreviewer rawSourcePreviewer;
    try {
      rawSourcePreviewer = (RawSourcePreviewer) previewerClass.newInstance();
      //inject values from url to fields in the rawSourcePreviewer
      for(ConfigDefinition confDef : configDefinitions) {
        Field f = previewerClass.getField(confDef.getFieldName());
        f.set(rawSourcePreviewer, previewParams.get(confDef.getName()).get(0));
      }
      rawSourcePreviewer.setMimeType(rawSourceDefinition.getMimeType());
      reader = rawSourcePreviewer.preview(bytesToRead);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }

    BoundedInputStream bIn = new BoundedInputStream(new ReaderInputStream(reader), bytesToRead);
    return Response.ok().type(rawSourcePreviewer.getMimeType()).entity(bIn).build();
  }

  private void validateParameters(MultivaluedMap<String, String> previewParams,
                                  List<ConfigDefinition> configDefinitions) throws PipelineRuntimeException {
    //validate that all configuration required by config definitions are supplied through the URL
    List<String> requiredPropertiesNotSet = new ArrayList<>();
    for(ConfigDefinition confDef: configDefinitions) {
      if(confDef.isRequired() && !previewParams.containsKey(confDef.getName())) {
        requiredPropertiesNotSet.add(confDef.getName());
      }
    }

    if(!requiredPropertiesNotSet.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(requiredPropertiesNotSet.get(0));
      for(int i = 1; i < requiredPropertiesNotSet.size(); i++) {
        sb.append(", ").append(requiredPropertiesNotSet.get(i));
      }
      throw new PipelineRuntimeException(ContainerErrors.CONTAINER_0160, sb.toString());
    }
  }

}