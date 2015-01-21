/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.config.CounterDefinition;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.SamplingDefinition;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

public class TestRuleResource  extends JerseyTest {

  private static final String PIPELINE_REV = "2.0";

  @Test
  public void testGetAlerts() {
    Response r = target("/v1/rules/myPipeline/alerts").queryParam("rev", PIPELINE_REV)
      .request().get();
    Assert.assertNotNull(r);
    List<AlertDefinition> alerts = r.readEntity(new GenericType<List<AlertDefinition>>() {});
    Assert.assertEquals(3, alerts.size());
  }

  @Test
  public void testSaveAlerts() {
    List<AlertDefinition> alerts = new ArrayList<>();
    alerts.add(new AlertDefinition("a1", "a1", "a", "2", null, true));
    alerts.add(new AlertDefinition("a2", "a1", "a", "2", null, true));
    alerts.add(new AlertDefinition("a3", "a1", "a", "2", null, true));

    Response r = target("/v1/rules/myPipeline/alerts").queryParam("rev", PIPELINE_REV).request()
      .post(Entity.json(alerts));
    List<AlertDefinition> result = r.readEntity(new GenericType<List<AlertDefinition>>() {});
    Assert.assertEquals(3, result.size());

  }

  @Test
  public void testGetMetricAlerts() {
    Response r = target("/v1/rules/myPipeline/metricAlerts").queryParam("rev", PIPELINE_REV)
      .request().get();
    Assert.assertNotNull(r);
    List<MetricsAlertDefinition> metricAlerts = r.readEntity(new GenericType<List<MetricsAlertDefinition>>() {});
    Assert.assertEquals(3, metricAlerts.size());
  }

  @Test
  public void testGetSamplingDefinitions() {
    Response r = target("/v1/rules/myPipeline/sampling").queryParam("rev", PIPELINE_REV)
      .request().get();
    Assert.assertNotNull(r);
    List<SamplingDefinition> sampling = r.readEntity(new GenericType<List<SamplingDefinition>>() {});
    Assert.assertEquals(3, sampling.size());
  }

  @Test
  public void testGetCounters() {
    Response r = target("/v1/rules/myPipeline/counters").queryParam("rev", PIPELINE_REV)
      .request().get();
    Assert.assertNotNull(r);
    List<CounterDefinition> counters = r.readEntity(new GenericType<List<CounterDefinition>>() {});
    Assert.assertEquals(3, counters.size());
  }

  @Test
  public void testSaveMetricAlerts() {
    List<MetricsAlertDefinition> metricsAlertDefinitions = new ArrayList<>();
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", true));

    Response r = target("/v1/rules/myPipeline/metricAlerts").queryParam("rev", PIPELINE_REV).request()
      .post(Entity.json(metricsAlertDefinitions));
    List<MetricsAlertDefinition> result = r.readEntity(new GenericType<List<MetricsAlertDefinition>>() {});
    Assert.assertEquals(3, result.size());
  }

  @Test
  public void testSaveSamplingDefinitions() {
    List<SamplingDefinition> samplingDefinitions = new ArrayList<>();
    samplingDefinitions.add(new SamplingDefinition("s1", "s1", "a", "2", null, true));
    samplingDefinitions.add(new SamplingDefinition("s2", "s2", "a", "2", null, true));
    samplingDefinitions.add(new SamplingDefinition("s3", "s3", "a", "2", null, true));

    Response r = target("/v1/rules/myPipeline/sampling").queryParam("rev", PIPELINE_REV).request()
      .post((Entity.json(samplingDefinitions)));
    List<SamplingDefinition> result = r.readEntity(new GenericType<List<SamplingDefinition>>() {});
    Assert.assertEquals(3, result.size());
  }

  @Test
  public void testSaveCounters() {
    List<CounterDefinition> counters = new ArrayList<>();
    counters.add(new CounterDefinition("c1", "c1", "l", "p", "g", true, null));
    counters.add(new CounterDefinition("c2", "c2", "l", "p", "g", true, null));
    counters.add(new CounterDefinition("c3", "c3", "l", "p", "g", true, null));

    Response r = target("v1/rules/myPipeline/counters").queryParam("rev", PIPELINE_REV).request()
      .post(Entity.json(counters));
    List<CounterDefinition> result = r.readEntity(new GenericType<List<CounterDefinition>>() {});
    Assert.assertEquals(3, result.size());
  }

  /*********************************************/
  /*********************************************/

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new RuleResourceConfig());
        register(RuleResource.class);
      }
    };
  }

  static class RuleResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(TestUtil.PipelineManagerTestInjector.class).to(ProductionPipelineManagerTask.class);
    }
  }
}
