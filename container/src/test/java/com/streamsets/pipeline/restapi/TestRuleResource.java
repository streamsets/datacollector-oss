/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.config.MetricDefinition;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.config.SamplingDefinition;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

public class TestRuleResource  extends JerseyTest {

  private static final String PIPELINE_REV = "2.0";


  @Test
  public void testSaveRules() {

    List<AlertDefinition> alerts = new ArrayList<>();
    alerts.add(new AlertDefinition("a1", "a1", "a", "2", ThresholdType.COUNT, "10", 100, true));
    alerts.add(new AlertDefinition("a2", "a1", "a", "2", ThresholdType.COUNT, "10", 100, true));
    alerts.add(new AlertDefinition("a3", "a1", "a", "2", ThresholdType.COUNT, "10", 100, true));

    List<MetricsAlertDefinition> metricsAlertDefinitions = new ArrayList<>();
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", true));

    List<SamplingDefinition> samplingDefinitions = new ArrayList<>();
    samplingDefinitions.add(new SamplingDefinition("s1", "s1", "a", "2", null, true));
    samplingDefinitions.add(new SamplingDefinition("s2", "s2", "a", "2", null, true));
    samplingDefinitions.add(new SamplingDefinition("s3", "s3", "a", "2", null, true));

    List<MetricDefinition> counters = new ArrayList<>();
    counters.add(new MetricDefinition("c1", "c1", "l", "p", "g", MetricType.METER, true));
    counters.add(new MetricDefinition("c2", "c2", "l", "p", "g", MetricType.METER, true));
    counters.add(new MetricDefinition("c3", "c3", "l", "p", "g", MetricType.METER, true));

    RuleDefinition ruleDefinition = new RuleDefinition(alerts, metricsAlertDefinitions, samplingDefinitions, counters);

    Response r = target("/v1/rules/myPipeline").queryParam("rev", PIPELINE_REV).request()
      .post(Entity.json(ruleDefinition));
    RuleDefinition result = r.readEntity(RuleDefinition.class);
    Assert.assertEquals(3, result.getAlertDefinitions().size());
    Assert.assertEquals(3, result.getMetricsAlertDefinitions().size());
    Assert.assertEquals(3, result.getSamplingDefinitions().size());
    Assert.assertEquals(3, result.getMetricDefinitions().size());
  }

  @Test
  public void testGetRules() {
    Response r = target("/v1/rules/myPipeline").queryParam("rev", PIPELINE_REV)
      .request().get();
    Assert.assertNotNull(r);
    RuleDefinition result = r.readEntity(RuleDefinition.class);
    Assert.assertEquals(3, result.getAlertDefinitions().size());
    Assert.assertEquals(3, result.getMetricsAlertDefinitions().size());
    Assert.assertEquals(3, result.getSamplingDefinitions().size());
    Assert.assertEquals(3, result.getMetricDefinitions().size());
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
