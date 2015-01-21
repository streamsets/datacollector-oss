/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.config.CounterDefinition;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.SamplingDefinition;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/v1/rules")
public class RuleResource {

  private final ProductionPipelineManagerTask pipelineManager;

  @Inject
  public RuleResource(ProductionPipelineManagerTask pipelineManager) {
    this.pipelineManager = pipelineManager;
  }

  @Path("/{name}/alerts")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAlerts(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev) {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getObserverStore().retrieveAlerts(name, rev)).build();

  }

  @Path("/{name}/alerts")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response saveAlerts(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    List<AlertDefinition> alerts) {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getObserverStore().storeAlerts(name, rev, alerts)).build();
  }

  @Path("/{name}/metricAlerts")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMetricAlerts(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev) {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getObserverStore().retrieveMetricAlerts(name, rev)).build();

  }

  @Path("/{name}/metricAlerts")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response saveMetricAlerts(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    List<MetricsAlertDefinition> metricsAlerts) {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getObserverStore().storeMetricAlerts(name, rev, metricsAlerts)).build();
  }

  @Path("/{name}/sampling")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSamplingDefinitions(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev) {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getObserverStore().retrieveSamplingDefinitions(name, rev)).build();

  }

  @Path("/{name}/sampling")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response saveSamplingDefinitions(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    List<SamplingDefinition> samplingDefinitions) {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getObserverStore().storeSamplingDefinitions(name, rev, samplingDefinitions)).build();
  }

  @Path("/{name}/counters")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCounters(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev) {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getObserverStore().retrieveCounters(name, rev)).build();

  }

  @Path("/{name}/counters")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response saveCounters(
    @PathParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    List<CounterDefinition> counters) {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getObserverStore().storeCounters(name, rev, counters)).build();
  }
}
