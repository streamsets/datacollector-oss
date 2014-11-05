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
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.preview.PreviewPipeline;
import com.streamsets.pipeline.runner.preview.PreviewPipelineBuilder;
import com.streamsets.pipeline.runner.preview.PreviewPipelineOutput;
import com.streamsets.pipeline.runner.preview.PreviewPipelineRunner;
import com.streamsets.pipeline.runner.preview.PreviewSourceOffsetTracker;
import com.streamsets.pipeline.stagelibrary.StageLibrary;
import com.streamsets.pipeline.store.PipelineStore;
import com.streamsets.pipeline.store.PipelineStoreException;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.Principal;
import java.util.Locale;

@Path("/v1/pipelines")
public class PreviewResource {
  private final Locale locale;
  private final PipelineStore store;
  private final StageLibrary stageLibrary;
  private final String user;



  @Inject
  public PreviewResource(Principal user, StageLibrary stageLibrary, PipelineStore store, Locale locale) {
    this.locale = locale;
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
      @QueryParam("batchSize") @DefaultValue("-1") int batchSize)
      throws PipelineStoreException, PipelineRuntimeException, StageException {
    PipelineConfiguration pipelineConf = store.load(name, rev);
    SourceOffsetTracker tracker = new PreviewSourceOffsetTracker(sourceOffset);
    PreviewPipelineRunner runner = new PreviewPipelineRunner(tracker);
    PreviewPipeline pipeline = new PreviewPipelineBuilder(stageLibrary, pipelineConf).build(runner);
    PreviewPipelineOutput previewOutput = pipeline.run();
    previewOutput.setLocale(locale);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewOutput).build();
  }

}
