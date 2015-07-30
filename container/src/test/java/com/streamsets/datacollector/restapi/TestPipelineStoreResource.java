/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.PipelineStoreResource;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.DataRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.MetricElementJson;
import com.streamsets.datacollector.restapi.bean.MetricTypeJson;
import com.streamsets.datacollector.restapi.bean.MetricsRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineInfoJson;
import com.streamsets.datacollector.restapi.bean.PipelineRevInfoJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.restapi.bean.StageConfigurationJson;
import com.streamsets.datacollector.restapi.bean.ThresholdTypeJson;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.RuleIssue;
import com.streamsets.datacollector.validation.ValidationError;

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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestPipelineStoreResource extends JerseyTest {

  @BeforeClass
  public static void beforeClass() {

  }

  @Test
  public void testGetPipelines() {
    Response response = target("/v1/pipelines").request().get();
    List<PipelineInfoJson> pipelineInfoJsons = response.readEntity(new GenericType<List<PipelineInfoJson>>() {});
    Assert.assertNotNull(pipelineInfoJsons);
    Assert.assertEquals(1, pipelineInfoJsons.size());
  }

  @Test
  public void testGetInfoPipeline() {
    Response response = target("/v1/pipeline/xyz").queryParam("rev", "1").queryParam("get", "pipeline").
        request().get();
    PipelineConfigurationJson pipelineConfig = response.readEntity(PipelineConfigurationJson.class);
    Assert.assertNotNull(pipelineConfig);
  }

  @Test
  public void testGetInfoInfo() {
    Response response = target("/v1/pipeline/xyz").queryParam("rev", "1.0.0").queryParam("get", "info").
        request().get();
    PipelineInfoJson pipelineInfoJson = response.readEntity(PipelineInfoJson.class);
    Assert.assertNotNull(pipelineInfoJson);
  }

  @Test
  public void testGetInfoHistory() {
    Response response = target("/v1/pipeline/xyz").queryParam("rev", "1.0.0").queryParam("get", "history").
        request().get();
    List<PipelineRevInfoJson> pipelineRevInfoJson = response.readEntity(new GenericType<List<PipelineRevInfoJson>>() {});
    Assert.assertNotNull(pipelineRevInfoJson);
  }

  @Test
  public void testCreate() {
    Response response = target("/v1/pipeline/myPipeline").queryParam("description", "my description").request()
        .put(Entity.json("abc"));
    PipelineConfigurationJson pipelineConfig = response.readEntity(PipelineConfigurationJson.class);
    Assert.assertEquals(201, response.getStatusInfo().getStatusCode());
    Assert.assertNotNull(pipelineConfig);
    Assert.assertNotNull(pipelineConfig.getUuid());
    Assert.assertEquals(3, pipelineConfig.getStages().size());

    StageConfigurationJson stageConf = pipelineConfig.getStages().get(0);
    Assert.assertEquals("s", stageConf.getInstanceName());
    Assert.assertEquals("sourceName", stageConf.getStageName());
    Assert.assertEquals("1", stageConf.getStageVersion());
    Assert.assertEquals("default", stageConf.getLibrary());

  }

  @Test
  public void testDelete() {
    Response response = target("/v1/pipeline/myPipeline").request().delete();
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testSave() {
    com.streamsets.datacollector.config.PipelineConfiguration toSave = MockStages.createPipelineConfigurationSourceProcessorTarget();
    Response response = target("/v1/pipeline/myPipeline")
        .queryParam("tag", "tag")
        .queryParam("tagDescription", "tagDescription").request()
        .post(Entity.json(BeanHelper.wrapPipelineConfiguration(toSave)));

    PipelineConfigurationJson returned = response.readEntity(PipelineConfigurationJson.class);
    Assert.assertNotNull(returned);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(toSave.getDescription(), returned.getDescription());

  }

  @Test
  public void testSaveRules() {

    List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons = new ArrayList<>();
    metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m1", "m1", "a", MetricTypeJson.COUNTER,
      MetricElementJson.COUNTER_COUNT, "p", false, true));
    metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m2", "m2", "a", MetricTypeJson.TIMER,
      MetricElementJson.TIMER_M15_RATE, "p", false, true));
    metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m3", "m3", "a", MetricTypeJson.HISTOGRAM,
      MetricElementJson.HISTOGRAM_MEAN, "p", false, true));

    List<DataRuleDefinitionJson> dataRuleDefinitionJsons = new ArrayList<>();
    dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("a", "a", "a", 20, 300, "x", true, "a", ThresholdTypeJson.COUNT, "200",
      1000, true, false, true));
    dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("b", "b", "b", 20, 300, "x", true, "a", ThresholdTypeJson.COUNT, "200",
      1000, true, false, true));
    dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("c", "c", "c", 20, 300, "x", true, "a", ThresholdTypeJson.COUNT, "200",
      1000, true, false, true));

    RuleDefinitionsJson ruleDefinitionsJson = new RuleDefinitionsJson(metricsRuleDefinitionJsons, dataRuleDefinitionJsons,
      Collections.<String>emptyList(), UUID.randomUUID());
    Response r = target("/v1/pipeline/myPipeline/rules").queryParam("rev", "tag").request()
      .post(Entity.json(ruleDefinitionsJson));
    RuleDefinitionsJson result = r.readEntity(RuleDefinitionsJson.class);

    Assert.assertEquals(3, result.getMetricsRuleDefinitions().size());
    Assert.assertEquals(3, result.getDataRuleDefinitions().size());
  }

  @Test
  public void testGetRules() {
    Response r = target("/v1/pipeline/myPipeline/rules").queryParam("rev", "tag")
      .request().get();
    Assert.assertNotNull(r);
    RuleDefinitionsJson result = r.readEntity(RuleDefinitionsJson.class);
    Assert.assertEquals(3, result.getMetricsRuleDefinitions().size());
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
      bindFactory(TestUtil.RuntimeInfoTestInjector.class).to(RuntimeInfo.class);
    }
  }

  static class PipelineStoreTestInjector implements Factory<PipelineStoreTask> {

    public PipelineStoreTestInjector() {
    }

    @Singleton
    @Override
    public PipelineStoreTask provide() {

      com.streamsets.datacollector.util.TestUtil.captureStagesForProductionRun();

      PipelineStoreTask pipelineStore = Mockito.mock(PipelineStoreTask.class);
      try {
        Mockito.when(pipelineStore.getPipelines()).thenReturn(ImmutableList.of(
            new com.streamsets.datacollector.store.PipelineInfo("name", "description", new java.util.Date(0), new java.util.Date(0), "creator",
                "lastModifier", "1", UUID.randomUUID(), true)));
        Mockito.when(pipelineStore.getInfo("xyz")).thenReturn(
            new com.streamsets.datacollector.store.PipelineInfo("xyz", "xyz description",new java.util.Date(0), new java.util.Date(0), "xyz creator",
                "xyz lastModifier", "1", UUID.randomUUID(), true));
        Mockito.when(pipelineStore.getHistory("xyz")).thenReturn(ImmutableList.of(
          new com.streamsets.datacollector.store.PipelineRevInfo(new com.streamsets.datacollector.store.PipelineInfo("xyz",
            "xyz description", new java.util.Date(0), new java.util.Date(0), "xyz creator",
                "xyz lastModifier", "1", UUID.randomUUID(), true))));
        Mockito.when(pipelineStore.load("xyz", "1")).thenReturn(
            MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.when(pipelineStore.create("nobody", "myPipeline", "my description")).thenReturn(
          MockStages.createPipelineConfigurationSourceProcessorTarget());
        Mockito.doNothing().when(pipelineStore).delete("myPipeline");
        Mockito.doThrow(new PipelineStoreException(ContainerError.CONTAINER_0200, "xyz"))
            .when(pipelineStore).delete("xyz");
        Mockito.when(pipelineStore.save(
            Matchers.anyString(), Matchers.anyString(), Matchers.anyString(), Matchers.anyString(),
            (com.streamsets.datacollector.config.PipelineConfiguration)Matchers.any())).thenReturn(
          MockStages.createPipelineConfigurationSourceProcessorTarget());

        List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons = new ArrayList<>();
        metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m1", "m1", "a", MetricTypeJson.COUNTER,
          MetricElementJson.COUNTER_COUNT, "p", false, true));
        metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m2", "m2", "a", MetricTypeJson.TIMER,
          MetricElementJson.TIMER_M15_RATE, "p", false, true));
        metricsRuleDefinitionJsons.add(new MetricsRuleDefinitionJson("m3", "m3", "a", MetricTypeJson.HISTOGRAM,
          MetricElementJson.HISTOGRAM_MEAN, "p", false, true));

        List<DataRuleDefinitionJson> dataRuleDefinitionJsons = new ArrayList<>();
        dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("a", "a", "a", 20, 300, "x", true, "c", ThresholdTypeJson.COUNT, "200",
          1000, true, false,true));
        dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("b", "b", "b", 20, 300, "x", true, "c", ThresholdTypeJson.COUNT, "200",
          1000, true, false, true));
        dataRuleDefinitionJsons.add(new DataRuleDefinitionJson("c", "c", "c", 20, 300, "x", true, "c", ThresholdTypeJson.COUNT, "200",
          1000, true, false, true));

        RuleDefinitionsJson rules = new RuleDefinitionsJson(metricsRuleDefinitionJsons, dataRuleDefinitionJsons,
          Collections.<String>emptyList(), UUID.randomUUID());
        List<RuleIssue> ruleIssues = new ArrayList<>();
        ruleIssues.add(RuleIssue.createRuleIssue("a", ValidationError.VALIDATION_0000));
        ruleIssues.add(RuleIssue.createRuleIssue("b", ValidationError.VALIDATION_0000));
        ruleIssues.add(RuleIssue.createRuleIssue("c", ValidationError.VALIDATION_0000));
        rules.getRuleDefinitions().setRuleIssues(ruleIssues);

        try {
          Mockito.when(pipelineStore.retrieveRules("myPipeline", "tag")).thenReturn(rules.getRuleDefinitions());
        } catch (PipelineStoreException e) {
          e.printStackTrace();
        }
        try {
          Mockito.when(pipelineStore.storeRules(
            Matchers.anyString(), Matchers.anyString(), (com.streamsets.datacollector.config.RuleDefinitions) Matchers.any()))
            .thenReturn(rules.getRuleDefinitions());
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
