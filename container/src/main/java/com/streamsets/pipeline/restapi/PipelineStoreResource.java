/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.PipelineStoreException;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;

@Path("/v1/pipeline-library")
public class PipelineStoreResource {
  private final PipelineStoreTask store;
  private final StageLibraryTask stageLibrary;
  private final URI uri;
  private final String user;



  @Inject
  public PipelineStoreResource(URI uri, Principal user, StageLibraryTask stageLibrary, PipelineStoreTask store) {
    this.uri = uri;
    this.user = user.getName();
    this.stageLibrary = stageLibrary;
    this.store = store;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPipelines() throws PipelineStoreException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(store.getPipelines()).build();
  }

  @Path("/{name}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getInfo(
      @PathParam("name") String name,
      @QueryParam("rev") String rev,
      @QueryParam("get") @DefaultValue("pipeline") String get,
      @QueryParam("attachment") @DefaultValue("false") Boolean attachment)
      throws PipelineStoreException, URISyntaxException {
    Object data;
    if (get.equals("pipeline")) {
      PipelineConfiguration pipeline = store.load(name, rev);
      PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipeline);
      validator.validate();
      pipeline.setValidation(validator);
      data = pipeline;
    } else if (get.equals("info")) {
      data = store.getInfo(name);
    } else if (get.equals("history")) {
      data = store.getHistory(name);
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid value for parameter 'get': {}", get));
    }

    if(attachment)
      return Response.ok().
        header("Content-Disposition", "attachment; filename=" + name + ".json").
        type(MediaType.APPLICATION_JSON).entity(data).build();
    else
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(data).build();

  }

  @Path("/{name}")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(
      @PathParam("name") String name,
      @QueryParam("description") @DefaultValue("") String description)
      throws PipelineStoreException, URISyntaxException {
    PipelineConfiguration pipeline = store.create(name, description, user);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipeline);
    validator.validate();
    pipeline.setValidation(validator);
    return Response.created(new URI(uri.toString() + "/" + name)).entity(pipeline).build();
  }

  @Path("/{name}")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(
      @PathParam("name") String name)
      throws PipelineStoreException, URISyntaxException {
    store.delete(name);
    return Response.ok().build();
  }

  @Path("/{name}")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response save(
      @PathParam("name") String name,
      @QueryParam("tag") String tag,
      @QueryParam("tagDescription") String tagDescription,
      PipelineConfiguration pipeline)
      throws PipelineStoreException, URISyntaxException {
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipeline);
    validator.validate();
    pipeline.setValidation(validator);
    pipeline = store.save(name, user, tag, tagDescription, pipeline);
    return Response.ok().entity(pipeline).build();
  }

}
