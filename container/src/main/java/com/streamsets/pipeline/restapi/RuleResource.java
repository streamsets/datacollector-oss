/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.store.PipelineStoreException;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/rules")
public class RuleResource {

  private final ProductionPipelineManagerTask pipelineManager;

  @Inject
  public RuleResource(ProductionPipelineManagerTask pipelineManager) {
    this.pipelineManager = pipelineManager;
  }

  @Path("/{name}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRules(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev) {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getObserverStore().retrieveRules(name, rev)).build();

  }

  @Path("/{name}")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response saveRules(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    RuleDefinition ruleDefinition) throws PipelineStoreException {
    RuleDefinition validatedRuleDefinition = pipelineManager.validateRuleDefinition(name, rev, ruleDefinition);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getObserverStore().storeRules(name, rev, validatedRuleDefinition)).build();
  }

  @Path("/{name}/deleteAlert")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteAlert(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @QueryParam("alertId") String alertId) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.deleteAlert(alertId)).build();
  }
}
