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

import com.streamsets.pipeline.config.RuntimePipelineConfiguration;
import com.streamsets.pipeline.store.PipelineStore;
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

@Path("/v1/pipeline")
public class PipelineStoreResource {
  private final PipelineStore store;
  private final URI uri;
  private final String user;

  @Inject
  public PipelineStoreResource(URI uri, Principal user, PipelineStore store) {
    this.uri = uri;
    this.user = user.getName();
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
      @QueryParam("rev") String rev, @QueryParam("get") @DefaultValue("pipeline") String get)
      throws PipelineStoreException, URISyntaxException {
    Object data;
    if (get.equals("pipeline")) {
      data = store.load(name, rev);
    } else if (get.equals("info")) {
      data = store.getInfo(name);
    } else if (get.equals("history")) {
      data = store.getHistory(name);
    } else {
      throw new IllegalArgumentException(String.format("Invalid value for parameter 'get': %s", get));
    }
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(data).build();
  }

  @Path("/{name}")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(
      @PathParam("name") String name,
      @QueryParam("description") @DefaultValue("") String description)
      throws PipelineStoreException, URISyntaxException {
    store.create(name, description, user);
    return Response.created(new URI(uri.toString() + "/" + name)).build();
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
      RuntimePipelineConfiguration pipeline)
      throws PipelineStoreException, URISyntaxException {
    store.save(name, user, tag, tagDescription, pipeline);
    return Response.ok().build();
  }

}
