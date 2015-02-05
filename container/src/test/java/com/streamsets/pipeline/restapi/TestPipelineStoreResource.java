/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineRevInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.validation.RuleIssue;
import com.streamsets.pipeline.validation.ValidationError;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestPipelineStoreResource extends JerseyTest {

  @BeforeClass
  public static void beforeClass() {

  }

  @Test
  public void testGetPipelines() {
    Response response = target("/v1/pipeline-library").request().get();
    List<PipelineInfo> pipelineInfos = response.readEntity(new GenericType<List<PipelineInfo>>() {});
    Assert.assertNotNull(pipelineInfos);
    Assert.assertEquals(1, pipelineInfos.size());
  }

  @Test
  public void testGetInfoPipeline() {
    Response response = target("/v1/pipeline-library/xyz").queryParam("rev", "1.0.0").queryParam("get", "pipeline").
        request().get();
    PipelineConfiguration pipelineConfig = response.readEntity(PipelineConfiguration.class);
    Assert.assertNotNull(pipelineConfig);
  }

  @Test
  public void testGetInfoInfo() {
    Response response = target("/v1/pipeline-library/xyz").queryParam("rev", "1.0.0").queryParam("get", "info").
        request().get();
    PipelineInfo pipelineInfo = response.readEntity(PipelineInfo.class);
    Assert.assertNotNull(pipelineInfo);
  }

  @Test
  public void testGetInfoHistory() {
    Response response = target("/v1/pipeline-library/xyz").queryParam("rev", "1.0.0").queryParam("get", "history").
        request().get();
    List<PipelineRevInfo> pipelineRevInfo = response.readEntity(new GenericType<List<PipelineRevInfo>>() {});
    Assert.assertNotNull(pipelineRevInfo);
  }

  @Test
  public void testCreate() {
    Response response = target("/v1/pipeline-library/myPipeline").queryParam("description", "my description").request()
        .put(Entity.json("abc"));
    PipelineConfiguration pipelineConfig = response.readEntity(PipelineConfiguration.class);
    Assert.assertEquals(201, response.getStatusInfo().getStatusCode());
    Assert.assertNotNull(pipelineConfig);
    Assert.assertNotNull(pipelineConfig.getUuid());
    Assert.assertEquals(3, pipelineConfig.getStages().size());

    StageConfiguration stageConf = pipelineConfig.getStages().get(0);
    Assert.assertEquals("s", stageConf.getInstanceName());
    Assert.assertEquals("sourceName", stageConf.getStageName());
    Assert.assertEquals("1.0.0", stageConf.getStageVersion());
    Assert.assertEquals("default", stageConf.getLibrary());

  }

  @Test
  public void testDelete() {
    Response response = target("/v1/pipeline-library/myPipeline").request().delete();
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testSave() {
    PipelineConfiguration toSave = MockStages.createPipelineConfigurationSourceProcessorTarget();
    Response response = target("/v1/pipeline-library/myPipeline")
        .queryParam("tag", "tag")
        .queryParam("tagDescription", "tagDescription").request()
        .post(Entity.json(toSave));

    PipelineConfiguration returned = response.readEntity(PipelineConfiguration.class);
    Assert.assertNotNull(returned);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(toSave.getDescription(), returned.getDescription());

  }

  @Test
  public void testSaveRules() {

    List<MetricsAlertDefinition> metricsAlertDefinitions = new ArrayList<>();
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", false, null, true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", false, null, true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", false, null, true));

    List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();
    dataRuleDefinitions.add(new DataRuleDefinition("a", "a", "a", 20, 300, "x", true, ThresholdType.COUNT, "200", 1000, true, false, null, true));
    dataRuleDefinitions.add(new DataRuleDefinition("b", "b", "b", 20, 300, "x", true, ThresholdType.COUNT, "200", 1000, true, false, null, true));
    dataRuleDefinitions.add(new DataRuleDefinition("c", "c", "c", 20, 300, "x", true, ThresholdType.COUNT, "200", 1000, true, false, null, true));

    RuleDefinition ruleDefinition = new RuleDefinition(metricsAlertDefinitions, dataRuleDefinitions);
    Response r = target("/v1/pipeline-library/myPipeline/rules").queryParam("rev", "tag").request()
      .post(Entity.json(ruleDefinition));
    RuleDefinition result = r.readEntity(RuleDefinition.class);

    Assert.assertEquals(3, result.getMetricsAlertDefinitions().size());
    Assert.assertEquals(3, result.getDataRuleDefinitions().size());
  }

  @Test
  public void testGetRules() {
    Response r = target("/v1/pipeline-library/myPipeline/rules").queryParam("rev", "tag")
      .request().get();
    Assert.assertNotNull(r);
    RuleDefinition result = r.readEntity(RuleDefinition.class);
    Assert.assertEquals(3, result.getMetricsAlertDefinitions().size());
    Assert.assertEquals(3, result.getDataRuleDefinitions().size());
  }

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new PipelineStoreResourceConfig());
        register(PipelineStoreResource.class);
      }
    };
  }

  static class PipelineStoreResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(PipelineStoreTestInjector.class).to(PipelineStoreTask.class);
      bindFactory(TestUtil.StageLibraryTestInjector.class).to(StageLibraryTask.class);
      bindFactory(TestUtil.PrincipalTestInjector.class).to(Principal.class);
      bindFactory(TestUtil.URITestInjector.class).to(URI.class);
    }
  }

  static class PipelineStoreTestInjector implements Factory<PipelineStoreTask> {

    public PipelineStoreTestInjector() {
    }

    @Singleton
    @Override
    public PipelineStoreTask provide() {

      com.streamsets.pipeline.util.TestUtil.captureStagesForProductionRun();

      PipelineStoreTask pipelineStore = Mockito.mock(PipelineStoreTask.class);
      try {
        Mockito.when(pipelineStore.getPipelines()).thenReturn(ImmutableList.of(
            new PipelineInfo("name", "description", new java.util.Date(0), new java.util.Date(0), "creator",
                "lastModifier", "1", UUID.randomUUID(), true)));
        Mockito.when(pipelineStore.getInfo("xyz")).thenReturn(
            new PipelineInfo("xyz", "xyz description",new java.util.Date(0), new java.util.Date(0), "xyz creator",
                "xyz lastModifier", "1", UUID.randomUUID(), true));
        Mockito.when(pipelineStore.getHistory("xyz")).thenReturn(ImmutableList.of(new PipelineRevInfo(
            new PipelineInfo("xyz", "xyz description", new java.util.Date(0), new java.util.Date(0), "xyz creator",
                "xyz lastModifier", "1", UUID.randomUUID(), true))));
        Mockito.when(pipelineStore.load("xyz", "1.0.0")).thenReturn(
            MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.when(pipelineStore.create("myPipeline", "my description", "nobody")).thenReturn(
            MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.doNothing().when(pipelineStore).delete("myPipeline");
        Mockito.doThrow(new PipelineStoreException(ContainerError.CONTAINER_0200, "xyz"))
            .when(pipelineStore).delete("xyz");
        Mockito.when(pipelineStore.save(
            Matchers.anyString(), Matchers.anyString(), Matchers.anyString(), Matchers.anyString(),
            (PipelineConfiguration)Matchers.any())).thenReturn(
          MockStages.createPipelineConfigurationSourceProcessorTarget());

        List<MetricsAlertDefinition> metricsAlertDefinitions = new ArrayList<>();
        metricsAlertDefinitions.add(new MetricsAlertDefinition("m1", "m1", "a", MetricType.COUNTER,
          MetricElement.COUNTER_COUNT, "p", false, null, true));
        metricsAlertDefinitions.add(new MetricsAlertDefinition("m2", "m2", "a", MetricType.TIMER,
          MetricElement.TIMER_M15_RATE, "p", false, null, true));
        metricsAlertDefinitions.add(new MetricsAlertDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
          MetricElement.HISTOGRAM_MEAN, "p", false, null, true));

        List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();
        dataRuleDefinitions.add(new DataRuleDefinition("a", "a", "a", 20, 300, "x", true, ThresholdType.COUNT, "200", 1000, true, false, null, true));
        dataRuleDefinitions.add(new DataRuleDefinition("b", "b", "b", 20, 300, "x", true, ThresholdType.COUNT, "200", 1000, true, false, null, true));
        dataRuleDefinitions.add(new DataRuleDefinition("c", "c", "c", 20, 300, "x", true, ThresholdType.COUNT, "200", 1000, true, false, null, true));

        RuleDefinition rules = new RuleDefinition(metricsAlertDefinitions, dataRuleDefinitions);
        List<RuleIssue> ruleIssues = new ArrayList<>();
        ruleIssues.add(RuleIssue.createRuleIssue("a", ValidationError.VALIDATION_0000));
        ruleIssues.add(RuleIssue.createRuleIssue("b", ValidationError.VALIDATION_0000));
        ruleIssues.add(RuleIssue.createRuleIssue("c", ValidationError.VALIDATION_0000));
        rules.setRuleIssues(ruleIssues);

        try {
          Mockito.when(pipelineStore.retrieveRules("myPipeline", "tag")).thenReturn(rules);
        } catch (PipelineStoreException e) {
          e.printStackTrace();
        }
        try {
          Mockito.when(pipelineStore.storeRules(
            Matchers.anyString(), Matchers.anyString(), (RuleDefinition) Matchers.any()))
            .thenReturn(rules);
        } catch (PipelineStoreException e) {
          e.printStackTrace();
        }

      } catch (PipelineStoreException e) {
        e.printStackTrace();
      }
      return pipelineStore;
    }

    @Override
    public void dispose(PipelineStoreTask pipelineStore) {
    }
  }

}
